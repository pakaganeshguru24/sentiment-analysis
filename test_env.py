import praw

reddit = praw.Reddit()
print("✅ Authenticated as:", reddit.user.me())
