

if __name__ == '__main__':

    def get_qoutes(keywords=('love', 'success', 'happiness', 'life', 'truth', 'pain', 'death')):
        from cachier import cachier
        import bs4
        import re

        @cachier()
        def _get(url):
            print(f'get: {url}')
            import requests
            return requests.get(url)

        # load all the qoutes for the different keywords
        qoutes = []
        for keyword in keywords:
            page = bs4.BeautifulSoup(_get(f'https://zenquotes.io/keywords/{keyword}').content, features="html.parser")
            qoutes.extend(qoute.text for qoute in page.find_all('blockquote', {'class': 'blockquote'}))
        qoutes = tuple(qoutes)

        @cachier()
        def _get_sentiments(qoutes):
            # sentiment analysis -- nltk
            import nltk
            import nltk.sentiment
            nltk.download('vader_lexicon')
            sia_nltk = nltk.sentiment.SentimentIntensityAnalyzer()
            # sentiment analysis -- flair
            import flair.models
            import flair.data
            sia_flair = flair.models.TextClassifier.load('en-sentiment')  # ~250MB
            # get sentiments...
            sentiments = []
            for text in qoutes:
                qoute, author = text.split(' — ')
                # apply nltk
                score_nltk = sia_nltk.polarity_scores(qoute)['compound']
                # apply flair
                sentance = flair.data.Sentence(qoute)
                sia_flair.predict(sentance)
                label_flair = sentance.labels[0]
                score_flair = (label_flair.score) if (label_flair.value == 'POSITIVE') else (-label_flair.score)
                # done
                sentiments.append((score_flair, score_nltk, qoute, author))
            return tuple(sorted(sentiments))

        # load all sentiments
        sentiments = _get_sentiments(qoutes)
        for sentiment in sentiments:
            print(sentiment)

    get_qoutes()
