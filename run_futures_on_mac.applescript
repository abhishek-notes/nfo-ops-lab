#!/usr/bin/osascript
-- AppleScript to run futures processing in a new Terminal window

tell application "Terminal"
    activate
    do script "cd /workspace && python3 pack_futures_simple.py > futures_processing_mac.log 2>&1"
end tell