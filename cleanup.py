from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk import sent_tokenize, word_tokenize
import string
import re
lemmatizer = WordNetLemmatizer()

try:
    stop = stopwords.words('english')
except:
    import nltk

    nltk.download('stopwords')
    stop = stopwords.words('english')
newstop_words = ['yolo']
stop.extend(newstop_words)

def cleanup(text, remove_punct=False):
    clean = ' '.join([word.lower() for word in text.split() if word.lower() not in (stop)])
    if remove_punct:
        clean = clean.translate(str.maketrans('', '', string.punctuation))
    clean = [lemmatizer.lemmatize(word) for word in clean.split()]
    return clean

def make_sentences_reddit(text):
    #cleanup whitespace
    text = text.replace('&nbsp;', '\n')
    text = re.sub(r'\n+', ' ', text).strip()
    text = re.sub(r'\s\s+', ' ', text).strip()

    #cleanup quotes
    text = re.sub(r'\"?\\?&?gt;?', '', text)

    # text = re.sub(r'\.+', '.', text).strip()
    # text = text.replace('\n', '. ')
    #turn headingings into new pharagraph
    text = re.sub(r'(\*\*+).*?(\*\*+)', lambda x: ". {}".format(x.group().replace('*', '')), text)
    text = re.sub(r'(\#+).*?(\#+)', lambda x: "{}".format(x.group().replace('#', '')), text)
    #text = text.replace('\n.', '')
    sentences = sent_tokenize(text)
    return sentences

def remove_urls(text):
    # cleanup hyperlinkes. If it is formatted remove the hyperlink
    text = re.sub(r'\(https?:\/\/[^\s]*\s?', ' ', text, flags=re.MULTILINE)
    return text