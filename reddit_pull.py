from datetime import datetime
import pandas as pd
import praw
from tqdm import tqdm
from string import punctuation
from config import get_creds


creds = get_creds()
reddit = praw.Reddit(
        client_id=creds["reddit_client_id"],
        client_secret=creds["reddit_client_secret"],
        user_agent=creds["reddit_user_agent"]
)
today = datetime.today().strftime("%Y-%m-%d")


def get_ticker(subreddits, limit=100):
    comments_lst = []
    threads_lst = []

    #"+".join(subreddits) would pull the top X across all subs in list
    subreddits = "+".join(subreddits)
    submissions = list(reddit.subreddit(subreddits).top("day", limit=limit))
    # limit = 3
    # counter = 0
    for submission in tqdm(submissions, position=1, leave=False, desc="submissions: {}".format(subreddits)):
        thread = {}
        thread["id"] = submission.id
        # title = submission.title
        thread["title"] = submission.title
        thread["date"] = datetime.fromtimestamp(submission.created).strftime("%Y-%m-%d")
        thread["award_cnt"] = submission.total_awards_received
        # for award in submission.all_awardings:
        #     thread["award_cnt"] += award['count']
        thread["up_cnt"] = submission.ups
        thread["down_cnt"] = submission.downs
        thread["body"] = submission.selftext
        thread["total_comments"] = submission.num_comments
        try:
            if submission.poll_data:
                for poll in submission.poll_data.options:
                    thread["body"] = "{}\n{}: {}".format(thread["body"], thread["title"], poll.text)
        except:
            pass
        thread["body"] = thread["body"].strip()
        thread["media"] = False
        if (submission.media is not None) or (not submission.is_self):
            thread["media"] = True
        thread["sub"] = submission.subreddit.display_name
        thread["url"] = "https://www.reddit.com{}".format(submission.permalink)
        thread["author"] = submission.author.name
        thread["author_karma"] = submission.author.awardee_karma
        # submission.comment_sort = "top"
        # submission.comment_limit = 100
        submission.comments.replace_more(limit=0)
        comments = submission.comments
        threads_lst.append(thread)
        for comment in tqdm(comments, position=1, leave=False, desc="comments: {}".format(subreddits)):
            comment_data = {}
            try:
                if comment.stickied:
                    continue
                comment_data["id"] = comment.id
                comment_data["date"] = datetime.fromtimestamp(comment.created).strftime("%Y-%m-%d")
                comment_data["body"] = comment.body
                comment_data["up_cnt"] = comment.ups
                comment_data["down_cnt"] = comment.downs
                comment_data["thread_id"] = submission.id
                comment_data["author"] = comment.author.name
                comment_data["author_karma"] = comment.author.awardee_karma
                comments_lst.append(comment_data)
            except Exception as e:
                print(e)
    thread_df = pd.DataFrame(threads_lst)
    comment_df = pd.DataFrame(comments_lst)
    return thread_df, comment_df


if __name__ == '__main__':
    subreddits = ["stocks", "investing", "smallstreetbets", "pennystocks", "weedstocks", "StockMarket",
                  "Trading", "Daytrading", "wallstreetbets"]
    thread_df_all = pd.DataFrame
    comment_df_all = pd.DataFrame
    for subreddit in tqdm(subreddits, position=0):
        print(subreddit)
        limit = 20
        if subreddit == "wallstreetbets":
            limit *= 2
        thread_df, comment_df = get_ticker([subreddit], limit=limit)
        if not thread_df_all.empty:
            thread_df_all = thread_df_all.append(thread_df)
            comment_df_all = comment_df_all.append(comment_df)
        else:
            thread_df_all = thread_df
            comment_df_all = comment_df
    thread_df_all.to_csv("data/thread_df-{}.csv".format(today), index=False)
    comment_df_all.to_csv("data/comment_df-{}.csv".format(today), index=False)

