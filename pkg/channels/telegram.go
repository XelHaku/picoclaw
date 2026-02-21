package channels

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/mymmrac/telego"
	"github.com/mymmrac/telego/telegohandler"
	th "github.com/mymmrac/telego/telegohandler"
	tu "github.com/mymmrac/telego/telegoutil"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/utils"
	"github.com/sipeed/picoclaw/pkg/voice"
)

// telegramAccount holds the runtime state for a single Telegram bot account.
type telegramAccount struct {
	id           string
	bot          *telego.Bot
	commands     TelegramCommander
	allowList    []string
	chatIDs      map[string]int64
	placeholders sync.Map // chatKey -> messageID
	stopThinking sync.Map // chatKey -> thinkingCancel
}

type TelegramChannel struct {
	*BaseChannel
	accounts    []*telegramAccount
	accountMap  map[string]*telegramAccount // id -> account (for outbound routing)
	config      *config.Config
	transcriber *voice.GroqTranscriber
}

type thinkingCancel struct {
	fn context.CancelFunc
}

func (c *thinkingCancel) Cancel() {
	if c != nil && c.fn != nil {
		c.fn()
	}
}

func NewTelegramChannel(cfg *config.Config, msgBus *bus.MessageBus) (*TelegramChannel, error) {
	telegramCfg := cfg.Channels.Telegram

	// Resolve account configs: prefer accounts[], fall back to legacy single token.
	accountConfigs := telegramCfg.Accounts
	if len(accountConfigs) == 0 && telegramCfg.Token != "" {
		accountConfigs = []config.TelegramAccountConfig{
			{
				ID:        "default",
				Token:     telegramCfg.Token,
				Proxy:     telegramCfg.Proxy,
				AllowFrom: telegramCfg.AllowFrom,
			},
		}
	}

	if len(accountConfigs) == 0 {
		return nil, fmt.Errorf("no telegram accounts configured")
	}

	accounts := make([]*telegramAccount, 0, len(accountConfigs))
	accountMap := make(map[string]*telegramAccount, len(accountConfigs))

	for _, acfg := range accountConfigs {
		id := strings.ToLower(strings.TrimSpace(acfg.ID))
		if id == "" {
			return nil, fmt.Errorf("telegram account has empty id")
		}
		if _, dup := accountMap[id]; dup {
			return nil, fmt.Errorf("duplicate telegram account id %q", id)
		}

		bot, err := newTelegramBot(acfg.Token, acfg.Proxy)
		if err != nil {
			return nil, fmt.Errorf("telegram account %q: %w", id, err)
		}

		acct := &telegramAccount{
			id:        id,
			bot:       bot,
			commands:  NewTelegramCommands(bot, cfg),
			allowList: acfg.AllowFrom,
			chatIDs:   make(map[string]int64),
		}
		accounts = append(accounts, acct)
		accountMap[id] = acct
	}

	// BaseChannel with empty allowList; per-account filtering is done in handleMessage.
	base := NewBaseChannel("telegram", telegramCfg, msgBus, nil)

	return &TelegramChannel{
		BaseChannel: base,
		accounts:    accounts,
		accountMap:  accountMap,
		config:      cfg,
	}, nil
}

func newTelegramBot(token, proxy string) (*telego.Bot, error) {
	var opts []telego.BotOption

	if proxy != "" {
		proxyURL, parseErr := url.Parse(proxy)
		if parseErr != nil {
			return nil, fmt.Errorf("invalid proxy URL %q: %w", proxy, parseErr)
		}
		opts = append(opts, telego.WithHTTPClient(&http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyURL(proxyURL),
			},
		}))
	} else if os.Getenv("HTTP_PROXY") != "" || os.Getenv("HTTPS_PROXY") != "" {
		opts = append(opts, telego.WithHTTPClient(&http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
			},
		}))
	}

	return telego.NewBot(token, opts...)
}

func (c *TelegramChannel) SetTranscriber(transcriber *voice.GroqTranscriber) {
	c.transcriber = transcriber
}

func (c *TelegramChannel) Start(ctx context.Context) error {
	logger.InfoCF("telegram", "Starting Telegram channel", map[string]any{
		"accounts": len(c.accounts),
	})

	for _, acct := range c.accounts {
		if err := c.startAccount(ctx, acct); err != nil {
			return fmt.Errorf("account %q: %w", acct.id, err)
		}
	}

	c.setRunning(true)
	return nil
}

func (c *TelegramChannel) startAccount(ctx context.Context, acct *telegramAccount) error {
	updates, err := acct.bot.UpdatesViaLongPolling(ctx, &telego.GetUpdatesParams{
		Timeout: 30,
	})
	if err != nil {
		return fmt.Errorf("failed to start long polling: %w", err)
	}

	bh, err := telegohandler.NewBotHandler(acct.bot, updates)
	if err != nil {
		return fmt.Errorf("failed to create bot handler: %w", err)
	}

	bh.HandleMessage(func(ctx *th.Context, message telego.Message) error {
		acct.commands.Help(ctx, message)
		return nil
	}, th.CommandEqual("help"))
	bh.HandleMessage(func(ctx *th.Context, message telego.Message) error {
		return acct.commands.Start(ctx, message)
	}, th.CommandEqual("start"))
	bh.HandleMessage(func(ctx *th.Context, message telego.Message) error {
		return acct.commands.Show(ctx, message)
	}, th.CommandEqual("show"))
	bh.HandleMessage(func(ctx *th.Context, message telego.Message) error {
		return acct.commands.List(ctx, message)
	}, th.CommandEqual("list"))
	bh.HandleMessage(func(ctx *th.Context, message telego.Message) error {
		return c.handleMessage(ctx, acct, &message)
	}, th.AnyMessage())

	logger.InfoCF("telegram", "Bot connected", map[string]any{
		"account":  acct.id,
		"username": acct.bot.Username(),
	})

	go bh.Start()
	go func() {
		<-ctx.Done()
		bh.Stop()
	}()

	return nil
}

func (c *TelegramChannel) Stop(ctx context.Context) error {
	logger.InfoC("telegram", "Stopping Telegram channel...")
	c.setRunning(false)
	return nil
}

func (c *TelegramChannel) Send(ctx context.Context, msg bus.OutboundMessage) error {
	if !c.IsRunning() {
		return fmt.Errorf("telegram channel not running")
	}

	accountID, rawChatID := splitChatKey(msg.ChatID)
	acct := c.resolveOutboundAccount(accountID)
	if acct == nil {
		return fmt.Errorf("no telegram account for chat key %q", msg.ChatID)
	}

	chatID, err := parseChatID(rawChatID)
	if err != nil {
		return fmt.Errorf("invalid chat ID %q: %w", rawChatID, err)
	}

	chatKey := msg.ChatID

	// Stop thinking animation
	if stop, ok := acct.stopThinking.Load(chatKey); ok {
		if cf, ok := stop.(*thinkingCancel); ok && cf != nil {
			cf.Cancel()
		}
		acct.stopThinking.Delete(chatKey)
	}

	htmlContent := markdownToTelegramHTML(msg.Content)

	// Try to edit placeholder
	if pID, ok := acct.placeholders.Load(chatKey); ok {
		acct.placeholders.Delete(chatKey)
		editMsg := tu.EditMessageText(tu.ID(chatID), pID.(int), htmlContent)
		editMsg.ParseMode = telego.ModeHTML

		if _, err = acct.bot.EditMessageText(ctx, editMsg); err == nil {
			return nil
		}
		// Fallback to new message if edit fails
	}

	tgMsg := tu.Message(tu.ID(chatID), htmlContent)
	tgMsg.ParseMode = telego.ModeHTML

	if _, err = acct.bot.SendMessage(ctx, tgMsg); err != nil {
		logger.ErrorCF("telegram", "HTML parse failed, falling back to plain text", map[string]any{
			"account": acct.id,
			"error":   err.Error(),
		})
		tgMsg.ParseMode = ""
		_, err = acct.bot.SendMessage(ctx, tgMsg)
		return err
	}

	return nil
}

// resolveOutboundAccount finds the account for an outbound message.
// If accountID matches, use it. If only one account exists, use that.
func (c *TelegramChannel) resolveOutboundAccount(accountID string) *telegramAccount {
	if acct, ok := c.accountMap[accountID]; ok {
		return acct
	}
	// Fallback: single-account mode
	if len(c.accounts) == 1 {
		return c.accounts[0]
	}
	return nil
}

func (c *TelegramChannel) handleMessage(ctx context.Context, acct *telegramAccount, message *telego.Message) error {
	if message == nil {
		return fmt.Errorf("message is nil")
	}

	user := message.From
	if user == nil {
		return fmt.Errorf("message sender (user) is nil")
	}

	senderID := fmt.Sprintf("%d", user.ID)
	if user.Username != "" {
		senderID = fmt.Sprintf("%d|%s", user.ID, user.Username)
	}

	// Per-account allowlist check
	if !isAllowedByList(senderID, acct.allowList) {
		logger.DebugCF("telegram", "Message rejected by allowlist", map[string]any{
			"account": acct.id,
			"user_id": senderID,
		})
		return nil
	}

	tgChatID := message.Chat.ID
	acct.chatIDs[senderID] = tgChatID

	content := ""
	mediaPaths := []string{}
	localFiles := []string{}

	defer func() {
		for _, file := range localFiles {
			if err := os.Remove(file); err != nil {
				logger.DebugCF("telegram", "Failed to cleanup temp file", map[string]any{
					"file":  file,
					"error": err.Error(),
				})
			}
		}
	}()

	if message.Text != "" {
		content += message.Text
	}

	if message.Caption != "" {
		if content != "" {
			content += "\n"
		}
		content += message.Caption
	}

	if len(message.Photo) > 0 {
		photo := message.Photo[len(message.Photo)-1]
		photoPath := downloadPhoto(ctx, acct.bot, photo.FileID)
		if photoPath != "" {
			localFiles = append(localFiles, photoPath)
			mediaPaths = append(mediaPaths, photoPath)
			if content != "" {
				content += "\n"
			}
			content += "[image: photo]"
		}
	}

	if message.Voice != nil {
		voicePath := downloadTelegramFile(ctx, acct.bot, message.Voice.FileID, ".ogg")
		if voicePath != "" {
			localFiles = append(localFiles, voicePath)
			mediaPaths = append(mediaPaths, voicePath)

			transcribedText := ""
			if c.transcriber != nil && c.transcriber.IsAvailable() {
				transcriberCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()

				result, err := c.transcriber.Transcribe(transcriberCtx, voicePath)
				if err != nil {
					logger.ErrorCF("telegram", "Voice transcription failed", map[string]any{
						"error": err.Error(),
						"path":  voicePath,
					})
					transcribedText = "[voice (transcription failed)]"
				} else {
					transcribedText = fmt.Sprintf("[voice transcription: %s]", result.Text)
					logger.InfoCF("telegram", "Voice transcribed successfully", map[string]any{
						"text": result.Text,
					})
				}
			} else {
				transcribedText = "[voice]"
			}

			if content != "" {
				content += "\n"
			}
			content += transcribedText
		}
	}

	if message.Audio != nil {
		audioPath := downloadTelegramFile(ctx, acct.bot, message.Audio.FileID, ".mp3")
		if audioPath != "" {
			localFiles = append(localFiles, audioPath)
			mediaPaths = append(mediaPaths, audioPath)
			if content != "" {
				content += "\n"
			}
			content += "[audio]"
		}
	}

	if message.Document != nil {
		docPath := downloadTelegramFile(ctx, acct.bot, message.Document.FileID, "")
		if docPath != "" {
			localFiles = append(localFiles, docPath)
			mediaPaths = append(mediaPaths, docPath)
			if content != "" {
				content += "\n"
			}
			content += "[file]"
		}
	}

	if content == "" {
		content = "[empty message]"
	}

	// Composite chat key: "accountID:telegramChatID"
	chatKey := buildChatKey(acct.id, tgChatID)

	logger.DebugCF("telegram", "Received message", map[string]any{
		"account":   acct.id,
		"sender_id": senderID,
		"chat_key":  chatKey,
		"preview":   utils.Truncate(content, 50),
	})

	// Thinking indicator
	err := acct.bot.SendChatAction(ctx, tu.ChatAction(tu.ID(tgChatID), telego.ChatActionTyping))
	if err != nil {
		logger.ErrorCF("telegram", "Failed to send chat action", map[string]any{
			"error": err.Error(),
		})
	}

	// Stop any previous thinking animation
	if prevStop, ok := acct.stopThinking.Load(chatKey); ok {
		if cf, ok := prevStop.(*thinkingCancel); ok && cf != nil {
			cf.Cancel()
		}
	}

	// Create cancel function for thinking state
	_, thinkCancel := context.WithTimeout(ctx, 5*time.Minute)
	acct.stopThinking.Store(chatKey, &thinkingCancel{fn: thinkCancel})

	pMsg, err := acct.bot.SendMessage(ctx, tu.Message(tu.ID(tgChatID), "Thinking... \U0001f4ad"))
	if err == nil {
		pID := pMsg.MessageID
		acct.placeholders.Store(chatKey, pID)
	}

	peerKind := "direct"
	peerID := fmt.Sprintf("%d", user.ID)
	if message.Chat.Type != "private" {
		peerKind = "group"
		peerID = fmt.Sprintf("%d", tgChatID)
	}

	metadata := map[string]string{
		"account_id": acct.id,
		"message_id": fmt.Sprintf("%d", message.MessageID),
		"user_id":    fmt.Sprintf("%d", user.ID),
		"username":   user.Username,
		"first_name": user.FirstName,
		"is_group":   fmt.Sprintf("%t", message.Chat.Type != "private"),
		"peer_kind":  peerKind,
		"peer_id":    peerID,
	}

	c.HandleMessage(fmt.Sprintf("%d", user.ID), chatKey, content, mediaPaths, metadata)
	return nil
}

// buildChatKey creates the composite chat key "accountID:chatID".
func buildChatKey(accountID string, chatID int64) string {
	return fmt.Sprintf("%s:%d", accountID, chatID)
}

// splitChatKey splits "accountID:chatID" into its parts.
// For plain numeric IDs (backward compat), accountID is empty.
func splitChatKey(chatKey string) (accountID, rawChatID string) {
	idx := strings.Index(chatKey, ":")
	if idx < 0 {
		// Plain numeric: legacy format
		return "", chatKey
	}
	prefix := chatKey[:idx]
	rest := chatKey[idx+1:]
	// If prefix looks numeric, it could be a negative chat ID (e.g. group IDs are negative).
	// Account IDs are always alphabetic, so a leading digit/minus means no prefix.
	if len(prefix) > 0 && (prefix[0] == '-' || (prefix[0] >= '0' && prefix[0] <= '9')) {
		return "", chatKey
	}
	return prefix, rest
}

// isAllowedByList checks if senderID is permitted by the given allowlist.
// Empty allowlist permits all.
func isAllowedByList(senderID string, allowList []string) bool {
	if len(allowList) == 0 {
		return true
	}

	idPart := senderID
	userPart := ""
	if idx := strings.Index(senderID, "|"); idx > 0 {
		idPart = senderID[:idx]
		userPart = senderID[idx+1:]
	}

	for _, allowed := range allowList {
		trimmed := strings.TrimPrefix(allowed, "@")
		allowedID := trimmed
		allowedUser := ""
		if idx := strings.Index(trimmed, "|"); idx > 0 {
			allowedID = trimmed[:idx]
			allowedUser = trimmed[idx+1:]
		}

		if senderID == allowed ||
			idPart == allowed ||
			senderID == trimmed ||
			idPart == trimmed ||
			idPart == allowedID ||
			(allowedUser != "" && senderID == allowedUser) ||
			(userPart != "" && (userPart == allowed || userPart == trimmed || userPart == allowedUser)) {
			return true
		}
	}

	return false
}

func downloadPhoto(ctx context.Context, bot *telego.Bot, fileID string) string {
	file, err := bot.GetFile(ctx, &telego.GetFileParams{FileID: fileID})
	if err != nil {
		logger.ErrorCF("telegram", "Failed to get photo file", map[string]any{
			"error": err.Error(),
		})
		return ""
	}
	return downloadFileWithInfo(bot, file, ".jpg")
}

func downloadFileWithInfo(bot *telego.Bot, file *telego.File, ext string) string {
	if file.FilePath == "" {
		return ""
	}
	fileURL := bot.FileDownloadURL(file.FilePath)
	logger.DebugCF("telegram", "File URL", map[string]any{"url": fileURL})
	filename := file.FilePath + ext
	return utils.DownloadFile(fileURL, filename, utils.DownloadOptions{
		LoggerPrefix: "telegram",
	})
}

func downloadTelegramFile(ctx context.Context, bot *telego.Bot, fileID, ext string) string {
	file, err := bot.GetFile(ctx, &telego.GetFileParams{FileID: fileID})
	if err != nil {
		logger.ErrorCF("telegram", "Failed to get file", map[string]any{
			"error": err.Error(),
		})
		return ""
	}
	return downloadFileWithInfo(bot, file, ext)
}

func parseChatID(chatIDStr string) (int64, error) {
	var id int64
	_, err := fmt.Sscanf(chatIDStr, "%d", &id)
	return id, err
}

func markdownToTelegramHTML(text string) string {
	if text == "" {
		return ""
	}

	codeBlocks := extractCodeBlocks(text)
	text = codeBlocks.text

	inlineCodes := extractInlineCodes(text)
	text = inlineCodes.text

	text = regexp.MustCompile(`^#{1,6}\s+(.+)$`).ReplaceAllString(text, "$1")

	text = regexp.MustCompile(`^>\s*(.*)$`).ReplaceAllString(text, "$1")

	text = escapeHTML(text)

	text = regexp.MustCompile(`\[([^\]]+)\]\(([^)]+)\)`).ReplaceAllString(text, `<a href="$2">$1</a>`)

	text = regexp.MustCompile(`\*\*(.+?)\*\*`).ReplaceAllString(text, "<b>$1</b>")

	text = regexp.MustCompile(`__(.+?)__`).ReplaceAllString(text, "<b>$1</b>")

	reItalic := regexp.MustCompile(`_([^_]+)_`)
	text = reItalic.ReplaceAllStringFunc(text, func(s string) string {
		match := reItalic.FindStringSubmatch(s)
		if len(match) < 2 {
			return s
		}
		return "<i>" + match[1] + "</i>"
	})

	text = regexp.MustCompile(`~~(.+?)~~`).ReplaceAllString(text, "<s>$1</s>")

	text = regexp.MustCompile(`^[-*]\s+`).ReplaceAllString(text, "\u2022 ")

	for i, code := range inlineCodes.codes {
		escaped := escapeHTML(code)
		text = strings.ReplaceAll(text, fmt.Sprintf("\x00IC%d\x00", i), fmt.Sprintf("<code>%s</code>", escaped))
	}

	for i, code := range codeBlocks.codes {
		escaped := escapeHTML(code)
		text = strings.ReplaceAll(
			text,
			fmt.Sprintf("\x00CB%d\x00", i),
			fmt.Sprintf("<pre><code>%s</code></pre>", escaped),
		)
	}

	return text
}

type codeBlockMatch struct {
	text  string
	codes []string
}

func extractCodeBlocks(text string) codeBlockMatch {
	re := regexp.MustCompile("```[\\w]*\\n?([\\s\\S]*?)```")
	matches := re.FindAllStringSubmatch(text, -1)

	codes := make([]string, 0, len(matches))
	for _, match := range matches {
		codes = append(codes, match[1])
	}

	i := 0
	text = re.ReplaceAllStringFunc(text, func(m string) string {
		placeholder := fmt.Sprintf("\x00CB%d\x00", i)
		i++
		return placeholder
	})

	return codeBlockMatch{text: text, codes: codes}
}

type inlineCodeMatch struct {
	text  string
	codes []string
}

func extractInlineCodes(text string) inlineCodeMatch {
	re := regexp.MustCompile("`([^`]+)`")
	matches := re.FindAllStringSubmatch(text, -1)

	codes := make([]string, 0, len(matches))
	for _, match := range matches {
		codes = append(codes, match[1])
	}

	i := 0
	text = re.ReplaceAllStringFunc(text, func(m string) string {
		placeholder := fmt.Sprintf("\x00IC%d\x00", i)
		i++
		return placeholder
	})

	return inlineCodeMatch{text: text, codes: codes}
}

func escapeHTML(text string) string {
	text = strings.ReplaceAll(text, "&", "&amp;")
	text = strings.ReplaceAll(text, "<", "&lt;")
	text = strings.ReplaceAll(text, ">", "&gt;")
	return text
}
