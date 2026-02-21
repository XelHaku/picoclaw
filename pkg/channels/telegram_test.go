package channels

import (
	"testing"
)

func TestBuildChatKey(t *testing.T) {
	got := buildChatKey("link", 6586915095)
	want := "link:6586915095"
	if got != want {
		t.Errorf("buildChatKey = %q, want %q", got, want)
	}
}

func TestSplitChatKey_Prefixed(t *testing.T) {
	tests := []struct {
		input     string
		wantAcct  string
		wantChatID string
	}{
		{"link:6586915095", "link", "6586915095"},
		{"yunobo:123456", "yunobo", "123456"},
		{"default:999", "default", "999"},
	}
	for _, tt := range tests {
		acct, chatID := splitChatKey(tt.input)
		if acct != tt.wantAcct || chatID != tt.wantChatID {
			t.Errorf("splitChatKey(%q) = (%q, %q), want (%q, %q)",
				tt.input, acct, chatID, tt.wantAcct, tt.wantChatID)
		}
	}
}

func TestSplitChatKey_PlainNumeric(t *testing.T) {
	// Plain numeric chat IDs (legacy format)
	tests := []struct {
		input     string
		wantAcct  string
		wantChatID string
	}{
		{"6586915095", "", "6586915095"},
		{"-100123456", "", "-100123456"},
	}
	for _, tt := range tests {
		acct, chatID := splitChatKey(tt.input)
		if acct != tt.wantAcct || chatID != tt.wantChatID {
			t.Errorf("splitChatKey(%q) = (%q, %q), want (%q, %q)",
				tt.input, acct, chatID, tt.wantAcct, tt.wantChatID)
		}
	}
}

func TestSplitChatKey_NegativeGroupID(t *testing.T) {
	// Prefixed with account, negative telegram group ID
	acct, chatID := splitChatKey("link:-100123456")
	if acct != "link" || chatID != "-100123456" {
		t.Errorf("splitChatKey(link:-100123456) = (%q, %q), want (link, -100123456)", acct, chatID)
	}
}

func TestIsAllowedByList_EmptyAllowsAll(t *testing.T) {
	if !isAllowedByList("anyone", nil) {
		t.Error("empty allowlist should allow all")
	}
	if !isAllowedByList("anyone", []string{}) {
		t.Error("empty allowlist should allow all")
	}
}

func TestIsAllowedByList_MatchByID(t *testing.T) {
	list := []string{"123456"}
	if !isAllowedByList("123456", list) {
		t.Error("should match by plain ID")
	}
	if !isAllowedByList("123456|john", list) {
		t.Error("should match compound sender by ID part")
	}
	if isAllowedByList("999999", list) {
		t.Error("should reject non-matching ID")
	}
}

func TestIsAllowedByList_MatchByUsername(t *testing.T) {
	list := []string{"@john"}
	if !isAllowedByList("123456|john", list) {
		t.Error("should match by username with @ prefix")
	}
	if isAllowedByList("123456|jane", list) {
		t.Error("should reject non-matching username")
	}
}
