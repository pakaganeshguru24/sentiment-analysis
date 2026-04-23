# Realtime Sentiment Analysis

This project extracts, transforms, analyzes, and visualizes sentiment data using the Reddit live api

## Reddit Login and Credentials

This project does not ship shared Reddit credentials.
Each user must create and use their own Reddit app credentials locally.

1. Go to https://www.reddit.com/prefs/apps
2. Create an app of type script
3. Copy .env.example to .env
4. Fill your own values in .env:
	- REDDIT_CLIENT_ID
	- REDDIT_CLIENT_SECRET
	- REDDIT_USER_AGENT
	- REDDIT_USERNAME
	- REDDIT_PASSWORD

Security note:
- Keep .env local only and never commit it to git.

