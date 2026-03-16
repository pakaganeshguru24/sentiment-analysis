import streamlit as st
import pandas as pd
import plotly.express as px
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from typing import List, Dict, Tuple
import io
import matplotlib.pyplot as plt

# Try to import WordCloud; if missing, we'll fallback to top-term bar chart
try:
    from wordcloud import WordCloud
    WORDCLOUD_AVAILABLE = True
except Exception:
    WORDCLOUD_AVAILABLE = False

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'extract'))
from extractors import extract_reddit_reviews


st.set_page_config(page_title="Topic Sentiment Search", layout="wide")
st.title("🔎 Topic Sentiment Search")
st.markdown("Enter a topic (product, tool, company, etc.), then click Search to fetch recent Reddit posts and analyze sentiment in real time.")

analyzer = SentimentIntensityAnalyzer()


@st.cache_data(ttl=300)
def fetch_data_for_topic(topic: str, reddit_limit: int = 50) -> List[Dict]:
    """Fetch data from Reddit for the given topic via the shared extractor."""
    try:
        return extract_reddit_reviews(topic, limit=reddit_limit)
    except Exception as e:
        st.error(f"Error fetching Reddit data: {e}")
        return []


def analyze_texts(items: List[Dict], source_name: str) -> pd.DataFrame:
    """Normalize extracted items and run VADER sentiment scoring.

    Expects each item to contain a textual field (content/text/title/selftext).
    """
    rows = []
    for it in items:
        # heuristics for text
        text = it.get('content') or it.get('text') or it.get('title') or ''
        if not text:
            continue
        scores = analyzer.polarity_scores(str(text))
        compound = scores.get('compound', 0.0)
        if compound >= 0.05:
            sentiment = 'Positive'
        elif compound <= -0.05:
            sentiment = 'Negative'
        else:
            sentiment = 'Neutral'

        rows.append({
            'source': source_name,
            'text': text,
            'sentiment': sentiment,
            'compound_score': compound,
            'raw': it,
        })

    return pd.DataFrame(rows)


def build_wordcloud(texts: List[str]):
    if not WORDCLOUD_AVAILABLE:
        return None
    joined = ' '.join(texts)
    wc = WordCloud(width=800, height=400, background_color='white').generate(joined)
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.imshow(wc, interpolation='bilinear')
    ax.axis('off')
    return fig


with st.sidebar:
    st.header('Search Controls')
    topic = st.text_input('Topic or product name', value='', placeholder='e.g., Samsung S25, ChatGPT, Tesla Model 3')
    reddit_limit = st.slider('Max Reddit posts', min_value=10, max_value=200, value=50, step=10)
    search_button = st.button('Search')

if not topic:
    st.info('Enter a topic in the sidebar and click Search to begin.')
else:
    # When Search clicked, fetch and analyze
    if search_button:
        with st.spinner('Fetching recent Reddit posts...'):
            reddit_posts = fetch_data_for_topic(topic, reddit_limit=reddit_limit)

        if not reddit_posts:
            st.warning('No Reddit data found for this topic.')
        else:
            df = analyze_texts(reddit_posts, 'Reddit')

            # Summary metrics
            st.subheader(f"Results for: {topic}")
            col1, col2, col3, col4 = st.columns([1,1,1,1])
            total = len(df)
            positives = (df['sentiment']=='Positive').sum()
            neutrals = (df['sentiment']=='Neutral').sum()
            negatives = (df['sentiment']=='Negative').sum()

            col1.metric('Total items', total)
            col2.metric('Positive', f"{positives} ({positives/total*100:.1f}%)" if total>0 else '0')
            col3.metric('Negative', f"{negatives} ({negatives/total*100:.1f}%)" if total>0 else '0')
            col4.metric('Source', 'Reddit')

            # Visuals: bar and pie
            st.markdown('### Sentiment Distribution')
            counts = df['sentiment'].value_counts().reindex(['Positive','Neutral','Negative']).fillna(0).astype(int)
            fig_bar = px.bar(x=counts.index, y=counts.values, labels={'x':'Sentiment','y':'Count'}, color=counts.index, color_discrete_map={'Positive':'#2ecc71','Neutral':'#95a5a6','Negative':'#e74c3c'})
            st.plotly_chart(fig_bar, use_container_width=True)

            fig_pie = px.pie(values=counts.values, names=counts.index, title='Sentiment Pie')
            st.plotly_chart(fig_pie, use_container_width=True)

            # Word cloud or top terms
            st.markdown('### Word Cloud / Top Terms')
            sample_texts = df['text'].astype(str).tolist()
            if WORDCLOUD_AVAILABLE and sample_texts:
                wc_fig = build_wordcloud(sample_texts)
                if wc_fig is not None:
                    st.pyplot(wc_fig)
            else:
                # fallback: show top terms
                from collections import Counter
                import re
                tokens = []
                for t in sample_texts:
                    words = re.findall(r"\w{3,}", t.lower())
                    tokens.extend([w for w in words if not w.isnumeric()])
                top = Counter(tokens).most_common(25)
                if top:
                    top_df = pd.DataFrame(top, columns=['term','count'])
                    fig_terms = px.bar(top_df, x='term', y='count', title='Top terms')
                    st.plotly_chart(fig_terms, use_container_width=True)
                else:
                    st.info('No frequent terms to show yet.')

            # Top posts/comments table
            st.markdown('### Top Posts / Comments')
            # Top positive
            pos_df = df[df['sentiment']=='Positive'].sort_values('compound_score', ascending=False).head(5)
            neg_df = df[df['sentiment']=='Negative'].sort_values('compound_score').head(5)

            t1, t2 = st.columns(2)
            with t1:
                st.markdown('#### Top Positive')
                if not pos_df.empty:
                    for _, r in pos_df.iterrows():
                        st.write(r['text'])
                        st.caption(f"Source: {r['source']} — Score: {r['compound_score']:.3f}")
                        st.divider()
                else:
                    st.info('No positive items found')

            with t2:
                st.markdown('#### Top Negative')
                if not neg_df.empty:
                    for _, r in neg_df.iterrows():
                        st.write(r['text'])
                        st.caption(f"Source: {r['source']} — Score: {r['compound_score']:.3f}")
                        st.divider()
                else:
                    st.info('No negative items found')

            # Raw table of top N
            st.markdown('### Recent Items (table)')
            display_df = df[['source','text','sentiment','compound_score']].copy()
            display_df['text'] = display_df['text'].str.slice(0,300)
            st.dataframe(display_df.head(100), use_container_width=True)

            st.success('Analysis complete')

    else:
        st.info('Press Search to fetch and analyze live data for the entered topic.')
