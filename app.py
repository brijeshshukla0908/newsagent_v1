import streamlit as st
import pandas as pd
from databricks import sql
import plotly.express as px
import re
from datetime import datetime
import time


# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="News Intelligence Dashboard",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# ENHANCED CSS
# ============================================================================

st.markdown("""
<style>
    /* Main styling */
    .main { background-color: #f8f9fa; }
  
    /* Headers with icons */
    h1 {
        color: #1f2937;
        border-bottom: 3px solid #3b82f6;
        padding-bottom: 10px;
        margin-bottom: 30px;
    }
  
    h2 {
        color: #374151;
        margin-top: 30px;
        padding-top: 15px;
        border-top: 2px solid #e5e7eb;
    }
  
    h3 {
        color: #4b5563;
        margin-top: 20px;
    }
  
    /* Loading spinner custom */
    .stSpinner > div {
        border-color: #3b82f6 !important;
    }
  
    /* Summary container with better spacing */
    .summary-container {
        background: white;
        padding: 35px;
        border-radius: 12px;
        box-shadow: 0 2px 12px rgba(0,0,0,0.08);
        margin: 25px 0;
        line-height: 1.9;
        color: #374151;
        border-left: 5px solid #3b82f6;
    }
  
    .summary-container h1 {
        color: #1f2937;
        font-size: 1.8rem;
        margin-bottom: 15px;
        border: none;
        padding: 0;
    }
  
    .summary-container h2 {
        color: #374151;
        font-size: 1.5rem;
        margin-top: 25px;
        margin-bottom: 12px;
        border: none;
        padding: 0;
    }
  
    .summary-container h3 {
        color: #4b5563;
        font-size: 1.25rem;
        margin-top: 20px;
        margin-bottom: 10px;
    }
  
    .summary-container p {
        margin-bottom: 15px;
        text-align: justify;
    }
  
    .summary-container ul, .summary-container ol {
        margin-left: 25px;
        margin-bottom: 15px;
    }
  
    .summary-container li {
        margin-bottom: 8px;
    }
  
    /* News card with hover effect */
    .news-card {
        background: white;
        padding: 25px;
        border-radius: 12px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        margin: 20px 0;
        border-left: 5px solid #3b82f6;
        transition: transform 0.2s, box-shadow 0.2s;
    }
  
    .news-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 16px rgba(0,0,0,0.12);
    }
  
    .news-headline {
        font-size: 1.35rem;
        font-weight: 700;
        color: #1f2937;
        margin-bottom: 15px;
        line-height: 1.4;
    }
  
    .news-summary {
        color: #4b5563;
        line-height: 1.7;
        margin-bottom: 20px;
        text-align: justify;
    }
  
    .news-meta {
        display: flex;
        gap: 25px;
        color: #6b7280;
        font-size: 0.9rem;
        margin-bottom: 15px;
        flex-wrap: wrap;
    }
  
    .news-meta span {
        display: flex;
        align-items: center;
        gap: 5px;
    }
  
    /* Sentiment badge */
    .sentiment-badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 12px;
        font-size: 0.85rem;
        font-weight: 600;
    }
  
    .sentiment-positive {
        background: #d1fae5;
        color: #065f46;
    }
  
    .sentiment-negative {
        background: #fee2e2;
        color: #991b1b;
    }
  
    .sentiment-neutral {
        background: #e5e7eb;
        color: #374151;
    }
  
    /* Empty state */
    .empty-state {
        text-align: center;
        padding: 60px 20px;
        background: white;
        border-radius: 12px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    }
  
    .empty-state-icon {
        font-size: 4rem;
        margin-bottom: 20px;
        opacity: 0.5;
    }
  
    .empty-state-text {
        color: #6b7280;
        font-size: 1.1rem;
    }
  
    /* Article HTML content */
    .article-html-content {
        line-height: 1.9;
        color: #374151;
        text-align: justify;
    }
  
    .article-html-content p {
        margin-bottom: 18px;
    }
  
    .article-html-content h1, .article-html-content h2, .article-html-content h3 {
        margin-top: 30px;
        margin-bottom: 15px;
        color: #1f2937;
    }
  
    .article-html-content img {
        max-width: 100%;
        height: auto;
        border-radius: 8px;
        margin: 20px 0;
    }
  
    /* Metric cards */
    div[data-testid="stMetricValue"] {
        font-size: 2.2rem;
        font-weight: 700;
    }
  
    /* Info boxes */
    .stInfo {
        background: linear-gradient(135deg, #dbeafe 0%, #bfdbfe 100%);
        border-left: 4px solid #3b82f6;
    }
  
    /* Success boxes */
    .stSuccess {
        background: linear-gradient(135deg, #d1fae5 0%, #a7f3d0 100%);
        border-left: 4px solid #10b981;
    }
  
    /* Warning boxes */
    .stWarning {
        background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%);
        border-left: 4px solid #f59e0b;
    }
    /* Pagination button fix */
    .stButton button {
        min-width: 60px !important;
        white-space: nowrap !important;
        text-overflow: clip !important;
    }

</style>
""", unsafe_allow_html=True)

# ============================================================================
# CONFIGURATION
# ============================================================================

DATABRICKS_SERVER_HOSTNAME = "dbc-4174758a-7c1d.cloud.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/fc454f4852c4fc93"                
DATABRICKS_TOKEN = "dapi7aae3d343c0f963365e3769adbf4a70b"

TIME_RANGES = {
    "today": "Today",
    "past2days": "Past 2 Days",
    "past7days": "Past Week",
    "past14days": "Past 2 Weeks",
    "past21days": "Past 3 Weeks",
    "past30days": "Past Month"
}

COMPANIES = [
    {"name": "Frequentis AG", "query": "3394046179"},
    {"name": "Lenzing AG", "query": "2232589230"},
    {"name": "Semperit AG Holdings", "query": "3085457182"},
    {"name": "AMAG Austria Metall AG", "query": "3202499507"},
    {"name": "PIERER Mobility AG", "query": "2764615227"},
    {"name": "Porr AG", "query": "2883282976"},
    {"name": "Wienerberger AG", "query": "1371594268"},
    {"name": "SBO AG", "query": "1801607662"},
    {"name": "Mayr-Melnhof Karton AG", "query": "2988873999"},
    {"name": "Kronospan Ltd.", "query": "0160053845"},
    {"name": "Binderholz GmbH", "query": "1079333987"}
]

ITEMS_PER_PAGE = 10

# ============================================================================
# DATABASE CONNECTION WITH RETRY
# ============================================================================

@st.cache_resource
def get_connection():
    """Get database connection with retry logic"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            conn = sql.connect(
                server_hostname=DATABRICKS_SERVER_HOSTNAME,
                http_path=DATABRICKS_HTTP_PATH,
                access_token=DATABRICKS_TOKEN,
                _connection_timeout=60
            )
            return conn
        except Exception as e:
            if attempt == max_retries - 1:
                st.error(f"❌ Connection failed after {max_retries} attempts: {e}")
                return None
            continue
    return None

# ============================================================================
# SESSION STATE
# ============================================================================

if 'page' not in st.session_state:
    st.session_state.page = 1

if 'show_modal' not in st.session_state:
    st.session_state.show_modal = False

if 'selected_article' not in st.session_state:
    st.session_state.selected_article = None

if 'search_query' not in st.session_state:
    st.session_state.search_query = ""

if 'current_view' not in st.session_state:
    st.session_state.current_view = "📰 News & Summaries"


# ============================================================================
# DATA LOADING WITH SPINNERS
# ============================================================================

@st.cache_data(ttl=300, show_spinner=False)
def load_company_summary(company, time_range_key):
    """Load company summary"""
    conn = get_connection()
    if not conn:
        return None
  
    query = f"""
        SELECT summary_text, article_count, generated_timestamp
        FROM workspace.default.company_summaries
        WHERE company_name = '{company}'
        AND date_range_key = '{time_range_key}'
        ORDER BY generated_timestamp DESC
        LIMIT 1
    """
  
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
      
        if result:
            return {'summary': result[0], 'count': result[1], 'timestamp': result[2]}
    except Exception as e:
        st.error(f"Error loading summary: {e}")
    return None

@st.cache_data(ttl=300, show_spinner=False)
def load_market_summary(time_range_key):
    """Load market summary"""
    conn = get_connection()
    if not conn:
        return None
  
    query = f"""
        SELECT summary_text, total_articles, companies_covered, generated_timestamp
        FROM workspace.default.market_summaries
        WHERE date_range_key = '{time_range_key}'
        ORDER BY generated_timestamp DESC
        LIMIT 1
    """
  
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
      
        if result:
            return {'summary': result[0], 'articles': result[1], 'companies': result[2], 'timestamp': result[3]}
    except Exception as e:
        st.error(f"Error loading market summary: {e}")
    return None

@st.cache_data(ttl=300, show_spinner=False)
def load_articles(company, time_range_key, search_query=""):
    """Load articles with optional search"""
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
  
    days_map = {"today": 0, "past2days": 2, "past7days": 7, "past14days": 14, "past21days": 21, "past30days": 30}
    days = days_map.get(time_range_key, 7)
  
    if days == 0:
        date_filter = "DATE(publication_date) = CURRENT_DATE"
    else:
        date_filter = f"publication_date >= CURRENT_DATE - INTERVAL {days} DAY"
  
    company_filter = "" if company == "All Companies" else f"AND company_name = '{company}'"
  
    search_filter = ""
    if search_query:
        search_filter = f"AND (LOWER(headline) LIKE '%{search_query.lower()}%' OR LOWER(article_summary) LIKE '%{search_query.lower()}%')"
  
    query = f"""
        SELECT company_name, headline, article_summary, body_clean, body_html,
               publication_date, sentiment_value, business_relevance, word_count
        FROM workspace.default.news_articles
        WHERE {date_filter}
        {company_filter}
        {search_filter}
        ORDER BY publication_date DESC, business_relevance DESC
    """
  
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Error loading articles: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def load_stats(time_range_key):
    """Load statistics"""
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
  
    days_map = {"today": 0, "past2days": 2, "past7days": 7, "past14days": 14, "past21days": 21, "past30days": 30}
    days = days_map.get(time_range_key, 7)
  
    if days == 0:
        date_filter = "DATE(publication_date) = CURRENT_DATE"
    else:
        date_filter = f"publication_date >= CURRENT_DATE - INTERVAL {days} DAY"
  
    query = f"""
        SELECT company_name, 
               COUNT(*) as article_count,
               ROUND(AVG(business_relevance), 2) as avg_relevance,
               SUM(CASE WHEN sentiment_value = 'POSITIVE' THEN 1 ELSE 0 END) as positive,
               SUM(CASE WHEN sentiment_value = 'NEGATIVE' THEN 1 ELSE 0 END) as negative,
               SUM(CASE WHEN sentiment_value = 'NEUTRAL' THEN 1 ELSE 0 END) as neutral
        FROM workspace.default.news_articles
        WHERE {date_filter}
        GROUP BY company_name
        ORDER BY article_count DESC
    """
  
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Error loading stats: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60, show_spinner=False)
def load_lifetime_cost():
    """Load lifetime cost"""
    conn = get_connection()
    if not conn:
        return None
  
    query = """
        SELECT 
            SUM(total_cost) as lifetime_cost,
            SUM(total_tokens) as lifetime_tokens,
            SUM(articles_processed) as total_articles,
            SUM(summaries_generated) as total_summaries
        FROM workspace.default.llm_cost_tracking
    """
  
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
      
        if result:
            return {'cost': result[0] or 0, 'tokens': result[1] or 0, 'articles': result[2] or 0, 'summaries': result[3] or 0}
    except Exception as e:
        st.error(f"Error loading cost data: {e}")
    return None

@st.cache_data(ttl=60, show_spinner=False)
def load_daily_costs():
    """Load daily costs"""
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
  
    query = """
        SELECT 
            run_date,
            operation_type,
            SUM(total_cost) as daily_cost,
            SUM(total_tokens) as daily_tokens
        FROM workspace.default.llm_cost_tracking
        WHERE run_date >= CURRENT_DATE - INTERVAL 30 DAY
        GROUP BY run_date, operation_type
        ORDER BY run_date DESC
    """
  
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Error loading daily costs: {e}")
        return pd.DataFrame()

# ============================================================================
# DISPLAY FUNCTIONS
# ============================================================================

def markdown_to_html(markdown_text):
    """Convert markdown to HTML"""
    if not markdown_text:
        return ""
  
    html = markdown_text
    html = re.sub(r'###\s+(.*?)(?=\n|$)', r'<h3>\1</h3>', html)
    html = re.sub(r'##\s+(.*?)(?=\n|$)', r'<h2>\1</h2>', html)
    html = re.sub(r'#\s+(.*?)(?=\n|$)', r'<h1>\1</h1>', html)
    html = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', html)
    html = re.sub(r'\*(.*?)\*', r'<em>\1</em>', html)
    html = re.sub(r'^\-\s+(.*?)$', r'<li>\1</li>', html, flags=re.MULTILINE)
    html = re.sub(r'(<li>.*?</li>)', r'<ul>\1</ul>', html, flags=re.DOTALL)
    html = re.sub(r'\n\n', r'</p><p>', html)
    html = f'<p>{html}</p>'
  
    return html

def display_summary(text):
    """Display summary with markdown rendering"""
    html = markdown_to_html(text)
    st.markdown(f"""
    <div class="summary-container">
        {html}
    </div>
    """, unsafe_allow_html=True)

def display_empty_state(message="No data available", icon="📭"):
    """Display empty state"""
    st.markdown(f"""
    <div class="empty-state">
        <div class="empty-state-icon">{icon}</div>
        <div class="empty-state-text">{message}</div>
    </div>
    """, unsafe_allow_html=True)

def get_sentiment_badge(sentiment):
    """Get sentiment badge HTML"""
    classes = {
        "POSITIVE": "sentiment-positive",
        "NEGATIVE": "sentiment-negative",
        "NEUTRAL": "sentiment-neutral"
    }
    return f'<span class="sentiment-badge {classes.get(sentiment, "sentiment-neutral")}">{sentiment}</span>'

def display_news_card(article, index):
    """Display news card"""
    sentiment_badge = get_sentiment_badge(article['sentiment_value'])
  
    st.markdown(f"""
    <div class="news-card">
        <div class="news-headline">{index}. {article['headline']}</div>
        <div class="news-meta">
            <span>📅 {article['publication_date']}</span>
            <span>🏢 {article['company_name']}</span>
            <span>📊 Relevance: {article['business_relevance']:.2f}</span>
            <span>{sentiment_badge}</span>
        </div>
        <div class="news-summary">{article['article_summary']}</div>
    </div>
    """, unsafe_allow_html=True)
  
    col1, col2, col3 = st.columns([6, 1, 1])
    with col3:
        if st.button("📖 Read Full", key=f"read_{index}", use_container_width=True):
            st.session_state.selected_article = article.to_dict()
            st.session_state.show_modal = True
            st.rerun()

def display_pagination(total_items, current_page, items_per_page):
    """Display pagination"""
    total_pages = (total_items + items_per_page - 1) // items_per_page
  
    if total_pages <= 1:
        return current_page
  
    col1, col2, col3, col4, col5 = st.columns([1, 1, 3, 1, 1])
  
    with col1:
        if st.button("⏮️ First", disabled=(current_page == 1), use_container_width=True):
            return 1
  
    with col2:
        if st.button("◀️ Prev", disabled=(current_page == 1), use_container_width=True):
            return current_page - 1
  
    with col3:
        if total_pages <= 7:
            page_range = range(1, total_pages + 1)
        else:
            if current_page <= 4:
                page_range = list(range(1, 6)) + ['...', total_pages]
            elif current_page >= total_pages - 3:
                page_range = [1, '...'] + list(range(total_pages - 4, total_pages + 1))
            else:
                page_range = [1, '...', current_page - 1, current_page, current_page + 1, '...', total_pages]
      
        cols = st.columns(min(7, total_pages))
        for i, page in enumerate(page_range[:7]):
            with cols[i]:
                if page == '...':
                    st.write("...")
                elif st.button(str(page), key=f"page_{page}", type="primary" if page == current_page else "secondary", use_container_width=True):
                    return page
  
    with col4:
        if st.button("Next ▶️", disabled=(current_page == total_pages), use_container_width=True):
            return current_page + 1
  
    with col5:
        if st.button("Last ⏭️", disabled=(current_page == total_pages), use_container_width=True):
            return total_pages
  
    return current_page

def show_article_modal():
    """Display article modal"""
    if not st.session_state.show_modal or st.session_state.selected_article is None:
        return
  
    article = st.session_state.selected_article
  
    with st.container():
        st.markdown("---")
        col1, col2, col3 = st.columns([1, 10, 1])
      
        with col2:
            st.markdown(f"### {article['headline']}")
          
            sentiment_badge = get_sentiment_badge(article['sentiment_value'])
          
            st.markdown(f"""
            <div style="color: #6b7280; margin-bottom: 25px;">
                📅 {article['publication_date']} | 🏢 {article['company_name']} | 📊 Relevance: {article['business_relevance']:.2f} | {sentiment_badge}
            </div>
            """, unsafe_allow_html=True)
          
            if pd.notna(article.get('body_html')) and article['body_html']:
                st.markdown(f"""
                <div class="article-html-content">
                    {article['body_html']}
                </div>
                """, unsafe_allow_html=True)
            elif pd.notna(article.get('body_clean')) and article['body_clean']:
                st.markdown(f"""
                <div class="article-html-content">
                    {article['body_clean'].replace(chr(10), '<br><br>')}
                </div>
                """, unsafe_allow_html=True)
            else:
                display_empty_state("Article content not available", "📄")
      
        with col3:
            if st.button("✖️ Close", use_container_width=True, type="primary"):
                st.session_state.show_modal = False
                st.session_state.selected_article = None
                st.rerun()
      
        st.markdown("---")


def get_relative_time(dt):
    """Convert datetime to relative time string (e.g., '2 hours ago')"""
    try:
        now = datetime.now()
        time_diff = now - dt
        total_seconds = time_diff.total_seconds()
      
        if total_seconds < 0:
            return "just now"
        elif total_seconds < 60:
            return "just now"
        elif total_seconds < 3600:  # Less than 1 hour
            minutes = int(total_seconds / 60)
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        elif total_seconds < 86400:  # Less than 24 hours
            hours = int(total_seconds / 3600)
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        elif total_seconds < 604800:  # Less than 7 days
            days = int(total_seconds / 86400)
            return f"{days} day{'s' if days != 1 else ''} ago"
        elif total_seconds < 2592000:  # Less than 30 days
            weeks = int(total_seconds / 604800)
            return f"{weeks} week{'s' if weeks != 1 else ''} ago"
        else:
            months = int(total_seconds / 2592000)
            return f"{months} month{'s' if months != 1 else ''} ago"
    except Exception as e:
        return "recently"




@st.cache_data(ttl=60, show_spinner=False)
def get_last_update_time():
    """Get the most recent update time across all tables"""
    conn = get_connection()
    if not conn:
        return None
  
    query = """
        SELECT 
            'Articles' as source,
            MAX(ingestion_timestamp) as last_update,
            COUNT(*) as count
        FROM workspace.default.news_articles
      
        UNION ALL
      
        SELECT 
            'Company Summaries' as source,
            MAX(generated_timestamp) as last_update,
            COUNT(*) as count
        FROM workspace.default.company_summaries
      
        UNION ALL
      
        SELECT 
            'Market Summaries' as source,
            MAX(generated_timestamp) as last_update,
            COUNT(*) as count
        FROM workspace.default.market_summaries
      
        ORDER BY last_update DESC
        LIMIT 1
    """
  
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
      
        if result and result[1]:
            source = result[0]
            last_update = result[1]
            count = result[2]
          
            # Convert to datetime if string
            if isinstance(last_update, str):
                try:
                    last_update = datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S.%f')
                except:
                    try:
                        last_update = datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S')
                    except:
                        return None
          
            # Remove timezone info if present
            if hasattr(last_update, 'tzinfo') and last_update.tzinfo is not None:
                last_update = last_update.replace(tzinfo=None)
          
            return {
                'source': source,
                'datetime': last_update,
                'count': count
            }
    except Exception as e:
        print(f"Error getting last update: {e}")
        return None
  
    return None



# ============================================================================
# MAIN APP
# ============================================================================

def main():
    # Get last update info
    last_update = get_last_update_time()
  
    # Header with relative time
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title("📰 News Intelligence Dashboard")
    with col2:
        if last_update:
            time_ago = get_relative_time(last_update['datetime'])
          
            st.markdown(f"""
            <div style="text-align: right; padding-top: 15px; color: #6b7280; font-size: 0.85rem;">
                <div style="color: #3b82f6; font-weight: 600;">Last Updated</div>
                <div style="font-weight: 600; color: #374151; font-size: 1rem; margin-top: 5px;">{time_ago}</div>
                <div style="font-size: 0.75rem; margin-top: 3px; color: #9ca3af;">({last_update['source']})</div>
            </div>
            """, unsafe_allow_html=True)
  
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Settings")
      
        main_view = st.radio(
            "📱 Dashboard",
            ["📰 News & Summaries", "💰 Cost Analytics"],
            index=0
        )
      
        # ✅ FIX: Auto-close modal when view changes
        if main_view != st.session_state.current_view:
            st.session_state.show_modal = False
            st.session_state.selected_article = None
            st.session_state.current_view = main_view
      
        if main_view == "📰 News & Summaries":
            st.markdown("---")
          
            time_label = st.selectbox("📅 Time Range", list(TIME_RANGES.values()), index=2)
            time_range_key = [k for k, v in TIME_RANGES.items() if v == time_label][0]
          
            company_names = ["All Companies"] + [c["name"] for c in COMPANIES]
            selected_company = st.selectbox("🏢 Company", company_names, index=0)
          
            view_type = st.radio("👁️ View", ["📊 Summary", "📰 News"], index=0)
          
            # Search in news view
            if view_type == "📰 News":
                st.markdown("---")
                st.session_state.search_query = st.text_input(
                    "🔍 Search articles", 
                    value=st.session_state.search_query, 
                    placeholder="Search headlines..."
                )
              
                # Clear search button
                if st.session_state.search_query:
                    if st.button("✖️ Clear Search", use_container_width=True):
                        st.session_state.search_query = ""
                        st.session_state.page = 1
                        st.rerun()
      
        st.markdown("---")
      
        # Refresh button
        if st.button("🔄 Refresh Data", use_container_width=True, type="primary"):
            st.cache_data.clear()
            st.session_state.page = 1
            st.rerun()
      
        # Info section with last update details
        if last_update:
            st.markdown("---")
            st.markdown("### 📊 Data Status")
          
            time_ago = get_relative_time(last_update['datetime'])
          
            st.caption(f"""
            **Last Update:** {time_ago}
            **Source:** {last_update['source']}
            **Items:** {last_update['count']:,}
            **Companies Tracked:** {len(COMPANIES)}
            """)

        st.markdown("---")
        st.caption("💡 **Tip:** Click refresh to reload latest data")
  
    # Show modal if active (will be auto-closed on view change)
    if st.session_state.show_modal:
        show_article_modal()
        return
  
    # Main content with loading spinner
    if main_view == "💰 Cost Analytics":
        with st.spinner("📊 Loading cost analytics..."):
            render_cost_dashboard()
    elif view_type == "📊 Summary":
        with st.spinner("📋 Loading summaries..."):
            render_summary_view(selected_company, time_range_key, time_label)
    else:
        with st.spinner("📰 Loading news articles..."):
            render_news_view(selected_company, time_range_key, time_label)

# ============================================================================
# SUMMARY VIEW
# ============================================================================

def render_summary_view(company, time_range_key, time_label):
  
    if company == "All Companies":
        st.subheader(f"🌐 Market Intelligence - {time_label}")
      
        market_summary = load_market_summary(time_range_key)
        stats = load_stats(time_range_key)
      
        if not stats.empty:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("📊 Companies", len(stats))
            with col2:
                st.metric("📰 Articles", f"{int(stats['article_count'].sum()):,}")
            with col3:
                st.metric("📈 Avg Relevance", f"{stats['avg_relevance'].mean():.2f}")
            with col4:
                positive_pct = (stats['positive'].sum() / stats['article_count'].sum() * 100) if stats['article_count'].sum() > 0 else 0
                st.metric("😊 Positive %", f"{positive_pct:.1f}%")
          
            st.markdown("---")
          
            if market_summary:
                st.markdown("### 📋 Market Intelligence Report")
                display_summary(market_summary['summary'])
                st.caption(f"📊 Based on {market_summary['articles']} articles • {market_summary['companies']} companies • Generated: {market_summary['timestamp']}")
            else:
                display_empty_state("Market summary not available for this period", "📊")
          
            st.markdown("---")
            st.markdown("### 📊 Company Breakdown")
          
            # Download button for stats
            csv = stats.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"company_stats_{time_label.replace(' ', '_')}.csv",
                mime="text/csv"
            )
          
            st.dataframe(stats, use_container_width=True, height=400)
          
            fig = px.bar(stats, x='company_name', y=['positive', 'negative', 'neutral'], 
                        title='Sentiment Distribution by Company',
                        color_discrete_map={'positive': '#10b981', 'negative': '#ef4444', 'neutral': '#6b7280'},
                        barmode='group')
            fig.update_layout(height=500, showlegend=True, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        else:
            display_empty_state("No data available for this period", "📭")
  
    else:
        st.subheader(f"🏢 {company} - {time_label}")
      
        company_summary = load_company_summary(company, time_range_key)
        articles = load_articles(company, time_range_key)
      
        if not articles.empty:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("📰 Articles", len(articles))
            with col2:
                st.metric("📈 Avg Relevance", f"{articles['business_relevance'].mean():.2f}")
            with col3:
                positive = len(articles[articles['sentiment_value'] == 'POSITIVE'])
                st.metric("🟢 Positive", positive)
            with col4:
                negative = len(articles[articles['sentiment_value'] == 'NEGATIVE'])
                st.metric("🔴 Negative", negative)
          
            st.markdown("---")
          
            if company_summary:
                st.markdown("### 📋 Executive Intelligence Report")
                display_summary(company_summary['summary'])
                st.caption(f"📊 Based on {company_summary['count']} articles • Generated: {company_summary['timestamp']}")
            else:
                display_empty_state("Executive summary not available for this period", "📊")
          
            st.markdown("---")
          
            col1, col2 = st.columns(2)
          
            with col1:
                sentiment_counts = articles['sentiment_value'].value_counts()
                fig = px.pie(values=sentiment_counts.values, names=sentiment_counts.index, 
                            title='Sentiment Distribution',
                            color=sentiment_counts.index, 
                            color_discrete_map={'POSITIVE': '#10b981', 'NEGATIVE': '#ef4444', 'NEUTRAL': '#6b7280'})
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
          
            with col2:
                # Relevance over time
                articles_copy = articles.copy()
                articles_copy['date'] = pd.to_datetime(articles_copy['publication_date']).dt.date
                daily_relevance = articles_copy.groupby('date')['business_relevance'].mean().reset_index()
              
                fig2 = px.line(daily_relevance, x='date', y='business_relevance', 
                              title='Average Relevance Over Time',
                              markers=True)
                fig2.update_traces(line_color='#3b82f6')
                fig2.update_layout(height=400)
                st.plotly_chart(fig2, use_container_width=True)
        else:
            display_empty_state(f"No articles found for {company} in the {time_label} period", "📭")

# ============================================================================
# NEWS VIEW
# ============================================================================

def render_news_view(company, time_range_key, time_label):
    st.subheader(f"📰 News Articles - {company} - {time_label}")
  
    articles = load_articles(company, time_range_key, st.session_state.search_query)
  
    if not articles.empty:
        total_articles = len(articles)
      
        if st.session_state.search_query:
            st.success(f"🔍 Found {total_articles} articles matching '{st.session_state.search_query}'")
        else:
            st.info(f"📊 Showing {total_articles} articles")
      
        current_page = st.session_state.page
        start_idx = (current_page - 1) * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
      
        paginated_articles = articles.iloc[start_idx:end_idx]
      
        for idx, article in paginated_articles.iterrows():
            display_news_card(article, start_idx + idx + 1)
      
        st.markdown("---")
        new_page = display_pagination(total_articles, current_page, ITEMS_PER_PAGE)
        if new_page != current_page:
            st.session_state.page = new_page
            st.rerun()
    else:
        if st.session_state.search_query:
            display_empty_state(f"No articles found matching '{st.session_state.search_query}'", "🔍")
        else:
            display_empty_state(f"No articles found for {company} in {time_label}", "📭")

# ============================================================================
# COST DASHBOARD
# ============================================================================

def render_cost_dashboard():
    st.subheader("💰 LLM Cost Analytics")
  
    lifetime = load_lifetime_cost()
  
    if lifetime:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("💵 Total Cost", f"${lifetime['cost']:.4f}")
        with col2:
            st.metric("🔢 Total Tokens", f"{lifetime['tokens']:,}")
        with col3:
            st.metric("📰 Articles", f"{lifetime['articles']:,}")
        with col4:
            st.metric("📝 Summaries", f"{lifetime['summaries']:,}")
      
        st.markdown("---")
      
        st.markdown("### 📅 Daily Cost Breakdown")
        daily_costs = load_daily_costs()
      
        if not daily_costs.empty:
            daily_chart = daily_costs.groupby('run_date')['daily_cost'].sum().reset_index()
            fig = px.line(daily_chart, x='run_date', y='daily_cost', 
                         title='Daily Cost Trend',
                         markers=True)
            fig.update_traces(line_color='#3b82f6', marker=dict(size=10))
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
          
            # Download button
            csv = daily_costs.to_csv(index=False)
            st.download_button(
                label="📥 Download Cost Report",
                data=csv,
                file_name=f"cost_report_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
          
            st.dataframe(
                daily_costs.style.format({'daily_cost': '${:.4f}', 'daily_tokens': '{:,.0f}'}),
                use_container_width=True,
                height=300
            )
          
            # Summary stats
            st.markdown("### 📊 Cost Summary")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Last 7 Days", f"${daily_costs.head(7)['daily_cost'].sum():.4f}")
            with col2:
                st.metric("Last 30 Days", f"${daily_costs['daily_cost'].sum():.4f}")
            with col3:
                avg_daily = daily_costs['daily_cost'].mean()
                st.metric("Avg Daily", f"${avg_daily:.4f}")
        else:
            display_empty_state("No cost data available yet", "💰")
    else:
        display_empty_state("Unable to load cost data", "⚠️")

# ============================================================================
# RUN
# ============================================================================

if __name__ == "__main__":
    main()


