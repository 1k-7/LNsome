#!/bin/bash

# Infinite loop to keep the bot running
while true; do
    echo "🚀 Starting Bot..."
    python bot.py
    
    echo "⚠️ Bot crashed or stopped! Restarting in 5 seconds..."
    echo "Press [CTRL+C] to stop locally."
    sleep 5
done