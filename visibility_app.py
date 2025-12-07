import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from supabase import create_client, Client
from streamlit_option_menu import option_menu
import extra_streamlit_components as stx
import time
from datetime import datetime, timedelta

# --- 1. CONFIGURATION & STYLING ---
st.set_page_config(
    page_title="AI Visibility by Virshi",
    page_icon="👁️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS to match the screenshots (Light gray background, card style, clean fonts)
st.markdown("""
<style>
    /* Main Background */
    .stApp {
        background-color: #F4F6F9;
    }
    
    /* Sidebar styling */
    section[data-testid="stSidebar"] {
        background-color: #FFFFFF;
        border-right: 1px solid #E0E0E0;
    }
    
    /* Card Styling */
    .css-1r6slb0, .css-12oz5g7 { 
        background-color: white;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        border: 1px solid #EAEAEA;
    }
    
    /* Metric Cards Customization */
    div[data-testid="stMetric"] {
        background-color: #ffffff;
        border: 1px solid #e0e0e0;
        padding: 15px;
        border-radius: 10px;
        text-align: center;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    
    /* Virshi Purple Accent */
    .stButton>button {
        background-color: #8041F6;
        color: white;
        border-radius: 8px;
        border: none;
    }
    .stButton>button:hover {
        background-color: #6a35cc;
    }
    
    /* Status Badges */
    .badge-trial {
        background-color: #FFECB3;
        color: #856404;
        padding: 4px 8px;
        border-radius: 4px;
        font-size: 0.8em;
        font-weight: bold;
    }
    .badge-active {
        background-color: #D4EDDA;
        color: #155724;
        padding: 4px 8px;
        border-radius: 4px;
        font-size: 0.8em;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# --- 2. SUPABASE & COOKIE SETUP ---

# Initialize Cookie Manager for Persistent Sessions
@st.cache_resource(experimental_allow_widgets=True)
def get_manager():
    return stx.CookieManager()

cookie_manager = get_manager()

# Initialize Supabase
try:
    SUPABASE_URL = st.secrets.get("SUPABASE_URL", {}).get("url", "https://placeholder.supabase.co")
    SUPABASE_KEY = st.secrets.get("SUPABASE_URL", {}).get("key", "placeholder")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    DB_CONNECTED = True if "placeholder" not in SUPABASE_URL else False
except:
    DB_CONNECTED = False
    
# Initialize Session State Variables
if 'user' not in st.session_state:
    st.session_state['user'] = None
if 'role' not in st.session_state:
    st.session_state['role'] = 'user'
if 'current_project' not in st.session_state:
    st.session_state['current_project'] = None

# --- 3. HELPER FUNCTIONS ---

def mock_login(email):
    """Simulate login for demo purposes if DB not connected"""
    return {
        "id": "mock-uuid-1234",
        "email": email,
        "role": "admin" if "admin" in email else "user"
    }

def get_donut_chart(value, title, color="#00C896"):
    """Generates a small donut chart for KPI cards similar to screenshots"""
    fig = go.Figure(data=[go.Pie(
        values=[value, 100-value],
        hole=.7,
        marker_colors=[color, '#F0F2F6'],
        textinfo='none',
        hoverinfo='none'
    )])
    fig.update_layout(
        showlegend=False,
        margin=dict(t=0, b=0, l=0, r=0),
        height=100,
        width=100,
        annotations=[dict(text=f"{value}%", x=0.5, y=0.5, font_size=16, showarrow=False, font_weight="bold")]
    )
    return fig

# --- 4. AUTHENTICATION LOGIC ---

def check_session():
    """Checks for existing session token in cookies"""
    if st.session_state['user'] is None:
        token = cookie_manager.get('virshi_token')
        if token and DB_CONNECTED:
            try:
                user = supabase.auth.get_user(token)
                if user:
                    st.session_state['user'] = user.user
                    # Fetch role logic here (omitted for brevity)
                    return True
            except:
                cookie_manager.delete('virshi_token')
        elif token and not DB_CONNECTED:
            # Restore mock session
            st.session_state['user'] = {"email": "demo@virshi.ai"}
            st.session_state['role'] = "admin"
            
def login_page():
    c1, c2, c3 = st.columns([1, 1.5, 1])
    with c2:
        st.image("https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png", width=200)
        st.markdown("<h2 style='text-align: center;'>Вхід в AI Visibility</h2>", unsafe_allow_html=True)
        
        with st.form("login_form"):
            email = st.text_input("Email", placeholder="name@company.com")
            password = st.text_input("Пароль", type="password")
            submit = st.form_submit_button("Увійти", use_container_width=True)
            
            if submit:
                if DB_CONNECTED:
                    try:
                        res = supabase.auth.sign_in_with_password({"email": email, "password": password})
                        st.session_state['user'] = res.user
                        cookie_manager.set('virshi_token', res.session.access_token, expires_at=datetime.now() + timedelta(days=7))
                        st.rerun()
                    except Exception as e:
                        st.error(f"Помилка входу: {e}")
                else:
                    # Mock Login
                    st.session_state['user'] = mock_login(email)
                    st.session_state['role'] = "admin" if "admin" in email else "user"
                    cookie_manager.set('virshi_token', 'mock_token', key="token_set")
                    st.success("Вхід успішний (Demo Mode)")
                    time.sleep(1)
                    st.rerun()

def onboarding_wizard():
    """Wizard for new users with 0 projects"""
    st.markdown("## 🚀 Налаштуємо ваш Brand Monitor")
    st.markdown("Ми згенеруємо перші аналітичні дані за 30 секунд.")
    
    with st.container(border=True):
        step = st.session_state.get('onboarding_step', 1)
        
        if step == 1:
            st.subheader("Крок 1: Інформація про Бренд")
            brand_name = st.text_input("Назва Бренду (напр. SkyUp)")
            domain = st.text_input("Домен сайту (напр. skyup.aero)")
            region = st.selectbox("Регіон пошуку", ["Ukraine", "USA", "Global"])
            
            if st.button("Далі"):
                if brand_name and domain:
                    st.session_state['temp_brand'] = brand_name
                    st.session_state['temp_domain'] = domain
                    st.session_state['onboarding_step'] = 2
                    st.rerun()
                else:
                    st.warning("Заповніть всі поля")
                    
        elif step == 2:
            st.subheader("Крок 2: AI Генерація запитів")
            st.write(f"Аналізуємо нішу для **{st.session_state['temp_brand']}**...")
            
            # Simulated Progress
            my_bar = st.progress(0)
            status_text = st.empty()
            
            for percent_complete in range(100):
                time.sleep(0.02)
                my_bar.progress(percent_complete + 1)
                if percent_complete < 30:
                    status_text.text("Генеруємо ключові слова через Gemini...")
                elif percent_complete < 70:
                    status_text.text("Скануємо видачу Perplexity...")
                else:
                    status_text.text("Розраховуємо сентимент та SOV...")
            
            st.success("Готово! Демо-проект створено.")
            if st.button("Перейти до Дашборду"):
                # In real app: Insert into DB here
                st.session_state['current_project'] = {
                    "name": st.session_state['temp_brand'],
                    "status": "trial",
                    "id": "new-proj"
                }
                st.rerun()

# --- 5. PAGE VIEWS ---

def show_dashboard():
    # Header
    col_head, col_status = st.columns([4, 1])
    with col_head:
        st.title(f"Дашборд: {st.session_state.get('current_project', {}).get('name', 'SkyUp')}")
    with col_status:
        status = st.session_state.get('current_project', {}).get('status', 'trial')
        if status == 'trial':
            st.markdown('<div style="text-align:right;"><span class="badge-trial">TRIAL MODE (5 Queries)</span></div>', unsafe_allow_html=True)
            if st.button("🚀 Upgrade to Pro"):
                st.toast("Зв'яжіться з менеджером для активації!")
        else:
            st.markdown('<div style="text-align:right;"><span class="badge-active">ACTIVE</span></div>', unsafe_allow_html=True)

    st.markdown("---")

    # KPI Grid (Row 1) - Based on Screenshot [image_a984f3.png]
    c1, c2, c3 = st.columns(3)
    
    with c1:
        with st.container(border=True):
            st.markdown("**ЧАСТКА ГОЛОСУ (SOV)**")
            col_kpi, col_chart = st.columns([1, 1])
            col_kpi.markdown("## 30.86%")
            col_chart.plotly_chart(get_donut_chart(30, "SOV"), use_container_width=True)

    with c2:
        with st.container(border=True):
            st.markdown("**% ОФІЦІЙНИХ ДЖЕРЕЛ**")
            col_kpi, col_chart = st.columns([1, 1])
            col_kpi.markdown("## 50.00%")
            col_chart.plotly_chart(get_donut_chart(50, "Off", "#00C896"), use_container_width=True)

    with c3:
        with st.container(border=True):
            st.markdown("**ЗАГАЛЬНИЙ НАСТРІЙ**")
            # Sentiment Pie Chart
            labels = ['Positive', 'Neutral', 'Negative']
            values = [10, 80, 10]
            fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0, marker_colors=['#00C896', '#9EA0A5', '#FF4B4B'])])
            fig.update_layout(height=120, margin=dict(t=0,b=0,l=0,r=0), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    # KPI Grid (Row 2)
    c4, c5, c6 = st.columns(3)
    
    with c4:
        with st.container(border=True):
            st.markdown("**ПОЗИЦІЯ БРЕНДУ**")
            st.markdown("<h1 style='text-align: center; color: #8041F6;'>1.0</h1>", unsafe_allow_html=True)
            st.progress(100) # Full bar for position 1

    with c5:
        with st.container(border=True):
            st.markdown("**ПРИСУТНІСТЬ БРЕНДУ**")
            col_kpi, col_chart = st.columns([1, 1])
            col_kpi.markdown("## 60.00%")
            col_chart.plotly_chart(get_donut_chart(60, "Pres"), use_container_width=True)

    with c6:
        with st.container(border=True):
            st.markdown("**ЗГАДКИ ДОМЕНУ**")
            col_kpi, col_chart = st.columns([1, 1])
            col_kpi.markdown("## 10.00%")
            col_chart.plotly_chart(get_donut_chart(10, "Dom"), use_container_width=True)

    # Main Chart: Brand Position Evolution
    st.markdown("### 📈 Динаміка Позицій (Brand Position Evolution)")
    with st.container(border=True):
        # Mock Data
        dates = pd.date_range(end=datetime.today(), periods=14)
        df = pd.DataFrame({
            "Date": dates,
            "SkyUp": [4, 3, 3, 2, 2, 1, 1, 2, 1, 1, 1, 1, 1, 1],
            "Ryanair": [2, 2, 1, 1, 3, 3, 2, 2, 3, 3, 2, 2, 3, 3]
        })
        fig = px.line(df, x="Date", y=["SkyUp", "Ryanair"], markers=True, 
                      color_discrete_map={"SkyUp": "#8041F6", "Ryanair": "#9EA0A5"})
        fig.update_layout(yaxis_autorange="reversed", template="plotly_white", height=350)
        st.plotly_chart(fig, use_container_width=True)

def show_competitors():
    st.title("⚔️ Аналіз Конкурентів")
    
    with st.container(border=True):
        c1, c2 = st.columns([3, 1])
        c1.markdown("Цей розділ показує, як ваші конкуренти ранжуються у відповідях AI порівняно з вами.")
        c2.button("Оновити дані", use_container_width=True)
        
        # Competitors Table
        data = {
            "Competitor": ["SkyUp", "Ryanair", "Wizz Air", "LOT"],
            "Avg Position": [1.0, 3.1, 2.9, 3.5],
            "Appearances": [50, 69, 73, 19],
            "Trend": ["⬆️", "⬆️", "⬇️", "—"]
        }
        df = pd.DataFrame(data)
        st.dataframe(
            df,
            column_config={
                "Avg Position": st.column_config.NumberColumn(format="%.1f"),
                "Appearances": st.column_config.ProgressColumn(format="%d", min_value=0, max_value=100),
            },
            hide_index=True,
            use_container_width=True
        )

def show_sources():
    st.title("📡 Джерела Даних (Sources)")
    
    tab1, tab2 = st.tabs(["Власні ресурси (Owned)", "Зовнішні медіа (Earned)"])
    
    with tab1:
        st.info("Сайти та соцмережі, які ви верифікували як офіційні.")
        st.dataframe(pd.DataFrame([
            {"Domain": "skyup.aero", "Mentions": 18, "Status": "Verified ✅"},
            {"Domain": "instagram.com/skyup", "Mentions": 11, "Status": "Verified ✅"}
        ]), use_container_width=True)
        
    with tab2:
        st.warning("Зовнішні сайти, які AI використовує як джерела інформації про вашу нішу.")
        st.dataframe(pd.DataFrame([
            {"Domain": "tripmydream.ua", "Mentions": 20, "Category": "Aggregator"},
            {"Domain": "lowcostavia.com.ua", "Mentions": 18, "Category": "News Blog"},
            {"Domain": "en.wikipedia.org", "Mentions": 5, "Category": "Wiki"}
        ]), use_container_width=True)

def show_recommendations():
    st.title("💡 AI Рекомендації")
    st.caption("Стратегії для покращення видимості на основі Gap-аналізу з лідером.")
    
    c1, c2, c3 = st.columns(3)
    
    with c1:
        st.subheader("📝 To Do")
        with st.container(border=True):
            st.markdown("**Website Collaboration**")
            st.caption("High Priority • PR")
            st.write("Tripmydream.ua цитує Ryanair, але не вас. Зв'яжіться з редакцією для додавання акцій.")
            st.button("Детальніше", key="r1")
            
    with c2:
        st.subheader("🚧 In Progress")
        with st.container(border=True):
            st.markdown("**Content Creation**")
            st.caption("Medium • Content")
            st.write("Створити FAQ сторінку про повернення квитків для Gemini.")
            st.progress(40)
            
    with c3:
        st.subheader("✅ Done")
        with st.container(border=True):
            st.markdown("**Technical Fix**")
            st.write("robots.txt оновлено для доступу GPTBot.")
            st.markdown("~~Виконано~~")

def show_admin():
    if st.session_state['role'] != 'admin':
        st.error("Доступ заборонено")
        return
        
    st.title("🛡️ Super Admin Panel")
    
    # KPIs for Admin
    k1, k2, k3 = st.columns(3)
    k1.metric("Всього Юзерів", "124")
    k2.metric("Активних Проектів", "85")
    k3.metric("Витрати Токенів (Сьогоді)", "1.2M")
    
    st.divider()
    
    st.subheader("Керування Клієнтами")
    
    # Mock User DB
    users_df = pd.DataFrame([
        {"email": "client@skyup.aero", "project": "SkyUp", "status": "trial", "tokens": 5000},
        {"email": "marketing@monobank.ua", "project": "Monobank", "status": "active", "tokens": 125000},
    ])
    
    for i, row in users_df.iterrows():
        with st.expander(f"{row['project']} ({row['email']})"):
            c1, c2, c3 = st.columns([2, 1, 1])
            c1.write(f"Tokens Used: {row['tokens']}")
            c2.write(f"Current Status: **{row['status'].upper()}**")
            
            if row['status'] == 'trial':
                if c3.button(f"Activate Pro", key=f"act_{i}"):
                    st.toast(f"Project {row['project']} activated!")
                    # SQL Update logic would go here

# --- 6. SIDEBAR & NAVIGATION ---

def sidebar_menu():
    with st.sidebar:
        # Logo
        st.image("https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png", width=250)
        
        # User Info
        if st.session_state['user']:
            user_email = st.session_state['user'].get('email', 'Guest')
            st.write(f"👤 **{user_email}**")
            if st.session_state.get('role') == 'admin':
                st.caption("🔴 SUPER ADMIN")
        
        # Navigation
        selected = option_menu(
            menu_title="Меню",
            options=["Дашборд", "Ключові слова", "Джерела", "Конкуренти", "Рекомендації", "Адмін"],
            icons=["speedometer2", "search", "hdd-network", "people", "lightbulb", "shield-lock"],
            menu_icon="cast",
            default_index=0,
            styles={
                "nav-link-selected": {"background-color": "#8041F6"},
            }
        )
        
        st.divider()
        if st.button("Вийти"):
            st.session_state['user'] = None
            cookie_manager.delete('virshi_token')
            st.rerun()
            
    return selected

# --- 7. MAIN APP ROUTER ---

def main():
    # 1. Check Cookies for Session
    check_session()
    
    # 2. Routing Logic
    if not st.session_state['user']:
        login_page()
    
    elif st.session_state.get('current_project') is None and st.session_state['role'] != 'admin':
        # New User Flow
        with st.sidebar:
            if st.button("Вийти"):
                st.session_state['user'] = None
                cookie_manager.delete('virshi_token')
                st.rerun()
        onboarding_wizard()
        
    else:
        # Main App Flow
        page = sidebar_menu()
        
        if page == "Дашборд":
            show_dashboard()
        elif page == "Ключові слова":
            st.title("🔍 Ключові слова (Query Explorer)")
            st.info("Тут буде список 5 демо-запитів з детальною аналітикою.")
        elif page == "Джерела":
            show_sources()
        elif page == "Конкуренти":
            show_competitors()
        elif page == "Рекомендації":
            show_recommendations()
        elif page == "Адмін":
            show_admin()

if __name__ == "__main__":
    main()
