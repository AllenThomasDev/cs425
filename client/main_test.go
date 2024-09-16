package main

import (
	"strconv"
	"strings"
	"testing"
)

func TestGrepOnVMs(t *testing.T) {
	testCases := []struct {
		name     string
		pattern  string
		expected int
	}{
		{"Common word", "the", 6},
		{"Rare word", "xylophone", 1},
		{"Non-existent word", "qwerty123", 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			results := BroadcastGrepRequest(tc.pattern+" -c", " ~/go_project/logs/*.log")

			totalCount := 0
			for vm, result := range results {
				output := result["output"].(string)
				count, err := strconv.Atoi(strings.TrimSpace(output))
				if err != nil {
					t.Errorf("Failed to parse count from VM %s: %v", vm, err)
					continue
				}
				totalCount += count
			}

			if totalCount != tc.expected {
				t.Errorf("Expected count %d for pattern '%s', but got %d", tc.expected, tc.pattern, totalCount)
			}
		})
	}
}
