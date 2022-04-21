from datetime import datetime, timedelta
import pandas as pd
import praw
from psaw import PushshiftAPI
from tqdm import tqdm
from string import punctuation
from config import get_creds
import traceback
import argparse


creds = get_creds()
reddit = praw.Reddit(
        client_id=creds["reddit_client_id"],
        client_secret=creds["reddit_client_secret"],
        user_agent=creds["reddit_user_agent"]
)
api = PushshiftAPI(reddit)


def check_available(post):
    try:
        good = (post.removed_by_category is None) and post.is_robot_indexable
    except Exception as e:
        good = (post.author is not None) and (post.author.name.lower is not "automoderator")
    return good


def get_ticker(subreddits, start_dt, end_dt, limit=100):
    comments_lst = []
    threads_lst = []

    #"+".join(subreddits) would pull the top X across all subs in list
    subreddits = "+".join(subreddits)
    submissions = list(api.search_submissions(subreddit=subreddits, before=end_dt, after=start_dt,
                                         limit=limit, sort_type="score"))
    #submissions = list(reddit.subreddit(subreddits).top("day", limit=limit))
    # limit = 3
    # counter = 0
    for submission in tqdm(submissions, position=1, leave=False, desc="submissions: {}".format(subreddits)):
        if not check_available(submission):
            continue
        thread = {}
        thread["id"] = submission.id
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
        try:
            thread["author"] = submission.author.name
            thread["author_karma"] = submission.author.awardee_karma
        except Exception as e:
            #the thread is still active, but the author isn't ex: https://www.reddit.com/r/smallstreetbets/comments/rt9m0z/apple_calls_and_puts_this_week_mostly_puts/
            continue
        # submission.comment_sort = "top"
        # submission.comment_limit = 100
        submission.comments.replace_more(limit=0)
        comments = submission.comments
        threads_lst.append(thread)
        for comment in tqdm(comments, position=1, leave=False, desc="comments: {}".format(subreddits)):
            if not check_available(comment):
                continue
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="date to run service for in %Y-%m-%d")
    args = parser.parse_args()


    subreddits = ["stocks", "investing", "smallstreetbets", "pennystocks", "weedstocks", "StockMarket",
                  "Trading", "Daytrading", "wallstreetbets"]
    thread_df_all = pd.DataFrame
    comment_df_all = pd.DataFrame

    if args.date:
        try:
            print(f"pulling for {args.date}")
            my_date = datetime.strptime(args.date, '%Y-%m-%d')
        except Exception as e:
            print("date should be in %Y-%m-%d format")
            exit(2)
    else:
        print(f"pulling for {datetime.today()}")
        my_date = datetime.today()
    start_dt = int((my_date-timedelta(days=1)).timestamp())
    end_dt = int(my_date.timestamp())

    try:
        for subreddit in tqdm(subreddits, position=0):
            print(subreddit)
            limit = 10
            if subreddit == "wallstreetbets":
                limit *= 2
            thread_df, comment_df = get_ticker([subreddit], start_dt, end_dt, limit=limit)
            if not thread_df_all.empty:
                thread_df_all = thread_df_all.append(thread_df)
                comment_df_all = comment_df_all.append(comment_df)
            else:
                thread_df_all = thread_df
                comment_df_all = comment_df
    except Exception as e:
        trace = traceback.format_exc()
        print("Uncaught exception: {}\n{}".format(e, trace))
        exit(1)
    run_date = datetime.utcfromtimestamp(start_dt).strftime("%Y-%m-%d")
    thread_df_all.to_csv("data/thread_df-{}.csv".format(run_date), index=False)
    comment_df_all.to_csv("data/comment_df-{}.csv".format(run_date), index=False)