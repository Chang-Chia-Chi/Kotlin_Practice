#!/bin/bash
# Telemetry Dashboard for Claude Code
# Parses live session data to display Token Usage and Rate Limits

# Read the JSON payload from standard input (passed by Claude Code /statusLine)
PAYLOAD=$(cat)

# Extract key metrics using jq
TOKENS=$(echo "$PAYLOAD" | jq -r '.token_usage // 0')
LIMITS=$(echo "$PAYLOAD" | jq -r '.rate_limits.five_hour // "N/A"')
MODEL=$(echo "$PAYLOAD" | jq -r '.model.display_name // "Unknown Model"')

# Output the formatted status line (You can add ANSI color codes here if your terminal supports it)
echo "🤖 Model: $MODEL | 📊 Session Tokens: $TOKENS / 150,000 Budget | ⏳ 5h Limit: $LIMITS"