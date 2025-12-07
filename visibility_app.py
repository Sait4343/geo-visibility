import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from supabase import create_client, Client
from streamlit_option_menu import option_menu
import extra_streamlit_components as stx
import time
import requests
from datetime import datetime, timedelta
import random

# --- 1. CONFIGURATION & STYLING ---
st.set_page_config(
    page_title="AI Visibility by Virshi",
    page_icon="👁️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .stApp { background-color: #F4F6F9; }
    section[data-testid="stSidebar"] { background-color: #FFFFFF; border-right: 1px solid #E0E0E0; }
    
    /* Cards */
    .css-1r6slb0, .css-12oz5g7 { 
        background-color: white; padding: 20px; border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05); border: 1px solid #EAEAEA;
    }
    
    /* Buttons & Badges */
    .stButton>button { background-color: #8041F6; color: white; border-radius: 8px; border: none; }
    .stButton>button:hover { background-color: #6a35cc; }
    
    .badge-trial { 
        background-color: #FFECB3; color: #856404; padding: 4px 8px; 
        border-radius: 4px; font-weight: bold; font-size: 0.75em; display: inline-block; margin-left: 5px;
    }
    .badge-active { 
        background-color: #D4EDDA; color: #155724; padding: 4px 8px; 
        border-radius: 4px; font-weight: bold; font-size: 0.75em; display: inline-block; margin-left: 5px;
    }
    
    /* Yellow Upgrade Button Style */
    .upgrade-btn {
        display: block;
        width: 100%;
        background-color: #FFC107;
        color: #000000;
        text-align: center;
        padding: 10px;
        border-radius: 8px;
        text-decoration: none;
        font-weight: bold;
        margin-top: 10px;
        border: 1px solid #e0a800;
    }
    .upgrade-btn:hover {
        background-color: #e0a800;
        color: #000000;
    }

    /* Sidebar Text */
    .sidebar-name { font-size: 16px; font-weight: 600; color: #333; }
    .sidebar-email { font-size: 13px; color: #666; margin-bottom: 5px;}
    .sidebar-label { font-size: 12px; color: #999; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 10px;}
</style>
""", unsafe_allow_html=True)

# --- 2. SETUP ---

cookie_manager = stx.CookieManager()

# Initialize Supabase
try:
    SUPABASE_URL = st.secrets.get("SUPABASE_URL", {}).get("url", "https://placeholder.supabase.co")
    SUPABASE_KEY = st.secrets.get("SUPABASE_URL", {}).get("key", "placeholder")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    DB_CONNECTED = True if "placeholder" not in SUPABASE_URL else False
except Exception:
    DB_CONNECTED = False
    
# Initialize Session State
if 'user' not in st.session_state:
    st.session_state['user'] = None
if 'user_details' not in st.session_state:
    st.session_state['user_details'] = {} # For First/Last name
if 'role' not in st.session_state:
    st.session_state['role'] = 'user'
if 'current_project' not in st.session_state:
    st.session_state['current_project'] = None
if 'gpt_history' not in st.session_state:
    st.session_state['gpt_history'] = []

# --- 3. HELPER FUNCTIONS ---

def mock_login(email):
    return {
        "id": "mock-uuid-1234",
        "email": email,
        "role": "admin" if "admin" in email else "user"
    }

def get_donut_chart(value, color="#00C896"):
    """Генерує динамічний графік на основі переданого значення"""
    remaining = max(0, 100 - value)
    fig = go.Figure(data=[go.Pie(
        values=[value, remaining],
        hole=.75,
        marker_colors=[color, '#F0F2F6'],
        textinfo='none',
        hoverinfo='label+percent'
    )])
    fig.update_layout(
        showlegend=False,
        margin=dict(t=0, b=0, l=0, r=0),
        height=80,
        width=80,
        annotations=[dict(text=f"{value}%", x=0.5, y=0.5, font_size=14, showarrow=False, font_weight="bold", font_color="#333")]
    )
    return fig

# Словник з підказками
METRIC_TOOLTIPS = {
    "sov": "Частка видимості вашого бренду у відповідях ШІ порівняно з конкурентами.",
    "official": "Частка посилань, які ведуть на ваші офіційні ресурси, серед усіх посилань про ваш бренд.",
    "sentiment": "Тональність, у якій ШІ описує бренд (Позитивна, Нейтральна або Негативна).",
    "position": "Середня позиція вашого бренду у відповідях ШІ.",
    "presence": "Відсоток запитів, у яких бренд був згаданий хоча б один раз.",
    "domain": "Відсоток запитів, у яких ШІ надав клікабельне посилання саме на ваш домен."
}

# --- 4. AUTHENTICATION ---

def check_session():
    if st.session_state['user'] is None:
        time.sleep(0.1)
        token = cookie_manager.get('virshi_token')
        
        if token and DB_CONNECTED:
            try:
                user = supabase.auth.get_user(token)
                if user:
                    st.session_state['user'] = user.user
                    # Тут треба також витягнути user_details з таблиці profiles
            except:
                cookie_manager.delete('virshi_token')
        elif token and not DB_CONNECTED:
            if token == 'mock_token_admin':
                st.session_state['user'] = {"email": "admin@virshi.ai"}
                st.session_state['user_details'] = {"first_name": "Super", "last_name": "Admin"}
                st.session_state['role'] = "admin"
            elif token.startswith('mock_token_user'):
                st.session_state['user'] = {"email": "client@skyup.aero"}
                # Спробуємо відновити ім'я з куки або дефолт
                st.session_state['user_details'] = {"first_name": "Іван", "last_name": "Клієнт"}
                st.session_state['role'] = "user"

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
                    role = "admin" if "admin" in email else "user"
                    st.session_state['user'] = mock_login(email)
                    st.session_state['role'] = role
                    # Default details for login flow (assuming they exist)
                    st.session_state['user_details'] = {"first_name": "Користувач", "last_name": ""}
                    
                    cookie_val = 'mock_token_admin' if role == 'admin' else 'mock_token_user_ex'
                    cookie_manager.set('virshi_token', cookie_val, key="token_set")
                    st.success(f"Вхід успішний")
                    time.sleep(1)
                    st.rerun()

def onboarding_wizard():
    st.markdown("## 🚀 Налаштуємо ваш Brand Monitor")
    with st.container(border=True):
        step = st.session_state.get('onboarding_step', 1)
        
        if step == 1:
            st.subheader("Крок 1: Про Вас")
            c1, c2 = st.columns(2)
            first_name = c1.text_input("Ім'я")
            last_name = c2.text_input("Прізвище")
            
            if st.button("Далі"):
                if first_name and last_name:
                    st.session_state['user_details'] = {"first_name": first_name, "last_name": last_name}
                    st.session_state['onboarding_step'] = 2
                    # Тут у реальному проекті треба зробити UPDATE profiles SET first_name=...
                    st.rerun()
                else:
                    st.warning("Будь ласка, введіть ім'я та прізвище")
                    
        elif step == 2:
            st.subheader("Крок 2: Інформація про Бренд")
            brand_name = st.text_input("Назва Бренду (напр. SkyUp)")
            domain = st.text_input("Домен сайту (напр. skyup.aero)")
            if st.button("Далі"):
                if brand_name and domain:
                    st.session_state['temp_brand'] = brand_name
                    st.session_state['temp_domain'] = domain
                    st.session_state['onboarding_step'] = 3
                    st.rerun()
                else:
                    st.warning("Заповніть поля")
                    
        elif step == 3:
            st.subheader("Крок 3: AI Сканування")
            st.write(f"Аналізуємо нішу для **{st.session_state['temp_brand']}**...")
            my_bar = st.progress(0)
            for p in range(100):
                time.sleep(0.01)
                my_bar.progress(p+1)
            st.success("Готово!")
            if st.button("Перейти до Дашборду"):
                st.session_state['current_project'] = {
                    "name": st.session_state['temp_brand'],
                    "status": "trial",
                    "created_at": datetime.now().strftime("%Y-%m-%d"),
                    "id": "new-proj"
                }
                st.rerun()

# --- 5. PAGE VIEWS ---

def show_dashboard():
    proj = st.session_state.get('current_project', {})
    
    # 1. Header with Time Filter
    c_title, c_filter = st.columns([3, 1])
    with c_title:
        st.title(f"Дашборд: {proj.get('name', 'SkyUp')}")
    with c_filter:
        time_range = st.selectbox("Період:", ["Останні 7 днів", "Останні 30 днів", "Останні 3 місяці"])
    
    st.markdown("---")
    
    # 2. Dynamic Data Generation (Mocking backend logic based on filter)
    # У реальному додатку тут буде SQL запит: WHERE created_at > now() - interval
    base_sov = 30.86
    base_off = 50.00
    base_pres = 60.00
    
    if time_range == "Останні 30 днів":
        base_sov += 2.5
        base_off -= 1.0
        base_pres += 5.0
    elif time_range == "Останні 3 місяці":
        base_sov += 5.2
        base_off += 5.0
        base_pres += 10.0
        
    # KPI Grid with Tooltips & Dynamic Charts
    c1, c2, c3 = st.columns(3)
    
    with c1:
        with st.container(border=True):
            st.markdown(f"**ЧАСТКА ГОЛОСУ (SOV)**", help=METRIC_TOOLTIPS["sov"])
            col_kpi, col_chart = st.columns([1, 1])
            col_kpi.markdown(f"## {base_sov:.2f}%")
            col_chart.plotly_chart(get_donut_chart(base_sov, "#00C896"), use_container_width=True)

    with c2:
        with st.container(border=True):
            st.markdown(f"**% ОФІЦІЙНИХ ДЖЕРЕЛ**", help=METRIC_TOOLTIPS["official"])
            col_kpi, col_chart = st.columns([1, 1])
            col_kpi.markdown(f"## {base_off:.2f}%")
            col_chart.plotly_chart(get_donut_chart(base_off, "#00C896"), use_container_width=True)

    with c3:
        with st.container(border=True):
            st.markdown(f"**ЗАГАЛЬНИЙ НАСТРІЙ**", help=METRIC_TOOLTIPS["sentiment"])
            # Dynamic Sentiment Pie
            pos = 15 if time_range == "Останні 7 днів" else 25
            neu = 75 if time_range == "Останні 7 днів" else 65
            neg = 10
            
            labels = ['Positive', 'Neutral', 'Negative']
            values = [pos, neu, neg]
            fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0, marker_colors=['#00C896', '#9EA0A5', '#FF4B4B'])])
            fig.update_layout(height=80, margin=dict(t=0,b=0,l=0,r=0), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    c4, c5, c6 = st.columns(3)
    
    with c4:
        with st.container(border=True):
            st.markdown(f"**ПОЗИЦІЯ БРЕНДУ**", help=METRIC_TOOLTIPS["position"])
            pos_val = 1.0 if time_range == "Останні 7 днів" else 1.4
            st.markdown(f"<h1 style='text-align: center; color: #8041F6;'>{pos_val}</h1>", unsafe_allow_html=True)
            st.progress(int(100 - (pos_val * 10))) # Visual bar

    with c5:
        with st.container(border=True):
            st.markdown(f"**ПРИСУТНІСТЬ БРЕНДУ**", help=METRIC_TOOLTIPS["presence"])
            col_kpi, col_chart = st.columns([1, 1])
            col_kpi.markdown(f"## {base_pres:.2f}%")
            col_chart.plotly_chart(get_donut_chart(base_pres, "#00C896"), use_container_width=True)

    with c6:
        with st.container(border=True):
            st.markdown(f"**ЗГАДКИ ДОМЕНУ**", help=METRIC_TOOLTIPS["domain"])
            col_kpi, col_chart = st.columns([1, 1])
            col_kpi.markdown("## 10.00%")
            col_chart.plotly_chart(get_donut_chart(10, "#00C896"), use_container_width=True)

    st.markdown("### 📈 Динаміка Позицій")
    
    # Dynamic Line Chart based on Filter
    days = 7 if "7" in time_range else (30 if "30" in time_range else 90)
    dates = pd.date_range(end=datetime.today(), periods=days)
    
    # Generate some random-ish but smooth data
    y_vals = [max(1, min(5, 3 + random.uniform(-1, 1))) for _ in range(days)]
    # Smoothing
    y_vals = pd.Series(y_vals).rolling(3, min_periods=1).mean()
    
    df = pd.DataFrame({"Date": dates, "Brand": y_vals})
    fig = px.line(df, x="Date", y="Brand", template="plotly_white")
    fig.update_yaxes(autorange="reversed") # Rank 1 is top
    st.plotly_chart(fig, use_container_width=True)

def show_gpt_visibility():
    st.title("🤖 GPT-Visibility Agent")
    for msg in st.session_state['gpt_history']:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])
            
    if prompt := st.chat_input("Запитайте щось..."):
        st.session_state['gpt_history'].append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.write(prompt)
        with st.chat_message("assistant"):
            with st.spinner("Аналізую..."):
                time.sleep(1)
                answer = "Це демо-відповідь агента."
            st.write(answer)
            st.session_state['gpt_history'].append({"role": "assistant", "content": answer})

def show_admin():
    if st.session_state['role'] != 'admin':
        st.error("Доступ заборонено")
        return
    st.title("🛡️ Super Admin Panel")
    st.dataframe(pd.DataFrame([
        {"Client": "SkyUp", "Status": "Trial", "Tokens": 5000},
        {"Client": "Mono", "Status": "Active", "Tokens": 120000}
    ]), use_container_width=True)

def show_competitors():
    st.title("⚔️ Конкуренти")

def show_sources():
    st.title("📡 Джерела")

def show_recommendations():
    st.title("💡 Рекомендації")

# --- 6. SIDEBAR & NAVIGATION ---

def sidebar_menu():
    with st.sidebar:
        st.image("https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png", width=250)
        
        # --- PROJECT SELECTOR (ADMIN ONLY) ---
        if st.session_state['role'] == 'admin':
            st.markdown("### 🛠 Admin: Select Client")
            proj_names = ["SkyUp", "Monobank", "Nova Poshta"]
            selected_name = st.selectbox("Перегляд проекту:", proj_names)
            # Mock update logic
            if not st.session_state.get('current_project') or st.session_state['current_project']['name'] != selected_name:
                st.session_state['current_project'] = {"name": selected_name, "status": "active" if selected_name != "SkyUp" else "trial", "created_at": "2025-01-01"}
        
        st.divider()

        # --- CURRENT PROJECT INFO & TRIAL BADGE ---
        if st.session_state.get('current_project'):
            proj = st.session_state['current_project']
            st.markdown(f"<div class='sidebar-label'>Current Brand</div>", unsafe_allow_html=True)
            
            # Name + Badge logic
            brand_display = f"**{proj['name']}**"
            if proj.get('status') == 'trial':
                brand_display += " <span class='badge-trial'>TRIAL MODE (5 Queries)</span>"
            elif proj.get('status') == 'active':
                brand_display += " <span class='badge-active'>PRO</span>"
                
            st.markdown(brand_display, unsafe_allow_html=True)
            
            st.markdown(f"<div class='sidebar-label'>Joined Date</div>", unsafe_allow_html=True)
            st.markdown(f"📅 {proj.get('created_at', 'N/A')}")
            
            # --- YELLOW UPGRADE BUTTON ---
            if proj.get('status') == 'trial':
                st.markdown(
                    f"""<a href="mailto:hi@virshi.ai?subject=Upgrade Plan Request for {proj['name']}" class="upgrade-btn">⭐ Підвищити план</a>""", 
                    unsafe_allow_html=True
                )
            
            st.divider()

        # --- MENU ---
        menu_options = ["Дашборд", "GPT-Visibility", "Ключові слова", "Джерела", "Конкуренти", "Рекомендації"]
        menu_icons = ["speedometer2", "robot", "search", "hdd-network", "people", "lightbulb"]
        
        if st.session_state['role'] == 'admin':
            menu_options.append("Адмін")
            menu_icons.append("shield-lock")

        selected = option_menu(
            menu_title=None,
            options=menu_options,
            icons=menu_icons,
            menu_icon="cast",
            default_index=0,
            styles={"nav-link-selected": {"background-color": "#8041F6"}}
        )
        
        st.divider()
        
        # --- USER INFO & LOGOUT ---
        if st.session_state['user']:
            # Get names from session
            details = st.session_state.get('user_details', {})
            first = details.get('first_name', '')
            last = details.get('last_name', '')
            email = st.session_state['user'].get('email')
            
            full_name = f"{first} {last}".strip() or "Користувач"
            
            st.markdown(f"<div class='sidebar-name'>{full_name}</div>", unsafe_allow_html=True)
            st.markdown(f"<div class='sidebar-email'>{email}</div>", unsafe_allow_html=True)
            
            if st.session_state.get('role') == 'admin':
                st.caption("🔴 SUPER ADMIN")
            
            st.markdown("---")
            st.markdown("**Support:** [hi@virshi.ai](mailto:hi@virshi.ai)")
            
            if st.button("Вийти"):
                st.session_state['user'] = None
                cookie_manager.delete('virshi_token')
                st.rerun()
            
    return selected

# --- 7. MAIN ---

def main():
    check_session()
    
    if not st.session_state['user']:
        login_page()
    elif st.session_state.get('current_project') is None and st.session_state['role'] != 'admin':
        with st.sidebar:
            if st.button("Вийти"):
                st.session_state['user'] = None
                cookie_manager.delete('virshi_token')
                st.rerun()
        onboarding_wizard()
    else:
        page = sidebar_menu()
        if page == "Дашборд": show_dashboard()
      
        elif page == "Ключові слова": st.title("🔍 Ключові слова"); st.info("Demo...")
        elif page == "Джерела": show_sources()
        elif page == "Конкуренти": show_competitors()
        elif page == "Рекомендації": show_recommendations()
        elif page == "Адмін": show_admin()
        elif page == "GPT-Visibility": show_gpt_visibility()     

if __name__ == "__main__":
    main()
