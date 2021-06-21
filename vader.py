from nltk.sentiment.vader import SentimentIntensityAnalyzer

def get_vader():
    try:
        vader = SentimentIntensityAnalyzer()
    except:
        import nltk
        nltk.download('vader_lexicon')
        vader = SentimentIntensityAnalyzer()
    return vader


def finviz_vader():
    vader = get_vader()
    new_words = {
            'crushes': 10,
            'beats': 5,
            'misses': -5,
            'trouble': -10,
            'fall': -100,
            'stomp': -10,
            'plunge': -100
        }
    vader.lexicon.update(new_words)
    return vader

def reddit_vader():
    vader = get_vader()
    return vader