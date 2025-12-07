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
    
    /* Sidebar Tweaks */
    section[data-testid="stSidebar"] { background-color: #FFFFFF; border-right: 1px solid #E0E0E0; }
    
    /* Зменшення відступів у сайдбарі та розміру лого */
    section[data-testid="stSidebar"] > div:first-child {
        padding-top: 1rem;
    }
    .sidebar-logo {
        margin-bottom: 0px;
        text-align: center;
    }
    
    /* Cards */
    .css-1r6slb0, .css-12oz5g7 { 
        background-color: white; padding: 20px; border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05); border: 1px solid #EAEAEA;
    }
    
    /* Buttons */
    .stButton>button { background-color: #8041F6; color: white; border-radius: 8px; border: none; }
    .stButton>button:hover { background-color: #6a35cc; }
    
    /* Upgrade Button (Yellow) */
    .upgrade-btn {
        display: block; width: 100%; background-color: #FFC107; color: #000000;
        text-align: center; padding: 10px; border-radius: 8px;
        text-decoration: none; font-weight: bold; margin-top: 10px; border: 1px solid #e0a800;
    }
    .upgrade-btn:hover { background-color: #e0a800; color: #000000; }

    /* Badges */
    .badge-trial { background-color: #FFECB3; color: #856404; padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 0.7em; }
    .badge-active { background-color: #D4EDDA; color: #155724; padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 0.7em; }
    
    /* Text styles */
    .sidebar-name { font-size: 14px; font-weight: 600; color: #333; margin-top: 5px;}
    .sidebar-label { font-size: 11px; color: #999; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 15px;}
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
    
# Session State
if 'user' not in st.session_state: st.session_state['user'] = None
if 'user_details' not in st.session_state: st.session_state['user_details'] = {} 
if 'role' not in st.session_state: st.session_state['role'] = 'user'
if 'current_project' not in st.session_state: st.session_state['current_project'] = None
if 'gpt_history' not in st.session_state: st.session_state['gpt_history'] = []
if 'generated_prompts' not in st.session_state: st.session_state['generated_prompts'] = []

# --- 3. HELPER FUNCTIONS ---

def mock_n8n_generate_prompts(brand, domain):
    """Імітація запиту до n8n, який повертає 10 промптів"""
    time.sleep(1.5) # Імітація затримки мережі
    return [
        f"Які авіакомпанії пропонують найдешевші квитки на сайті {domain}?",
        f"Відгуки про сервіс {brand} 2025",
        f"Як купити квитки {brand} онлайн?",
        f"Правила перевезення багажу {brand}",
        f"Акції та знижки {brand} на цей місяць",
        f"Порівняння цін {brand} та конкурентів",
        f"Чи надійна компанія {brand}?",
        f"Контакти підтримки {domain}",
        f"Мобільний додаток {brand} огляд",
        f"Історія компанії {brand}"
    ]

def get_donut_chart(value, color="#00C896"):
    remaining = max(0, 100 - value)
    fig = go.Figure(data=[go.Pie(
        values=[value, remaining], hole=.75,
        marker_colors=[color, '#F0F2F6'], textinfo='none', hoverinfo='label+percent'
    )])
    fig.update_layout(
        showlegend=False, margin=dict(t=0, b=0, l=0, r=0), height=80, width=80,
        annotations=[dict(text=f"{value}%", x=0.5, y=0.5, font_size=14, showarrow=False, font_weight="bold", font_color="#333")]
    )
    return fig

METRIC_TOOLTIPS = {
    "sov": "Частка видимості вашого бренду у відповідях ШІ порівняно з конкурентами.",
    "official": "Частка посилань на ваші офіційні ресурси.",
    "sentiment": "Тональність: Позитивна, Нейтральна або Негативна.",
    "position": "Середня позиція вашого бренду у списках рекомендацій.",
    "presence": "Відсоток запитів, де бренд був згаданий.",
    "domain": "Відсоток запитів з клікабельним посиланням на ваш домен."
}

# --- 4. AUTH & ONBOARDING ---

def check_session():
    if st.session_state['user'] is None:
        time.sleep(0.1)
        token = cookie_manager.get('virshi_token')
        if token and DB_CONNECTED:
            try:
                user = supabase.auth.get_user(token)
                if user: st.session_state['user'] = user.user
            except: cookie_manager.delete('virshi_token')
        elif token and not DB_CONNECTED:
            # Mock login restoration
            if token == 'mock_admin':
                st.session_state['user'] = {"email": "admin@virshi.ai"}
                st.session_state['user_details'] = {"first_name": "Super", "last_name": "Admin"}
                st.session_state['role'] = "admin"
            elif token.startswith('mock_user'):
                st.session_state['user'] = {"email": "client@skyup.aero"}
                st.session_state['user_details'] = {"first_name": "Іван", "last_name": "Петренко"}
                st.session_state['role'] = "user"

def login_page():
    c1, c2, c3 = st.columns([1, 1.5, 1])
    with c2:
        st.image("https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png", width=180)
        st.markdown("<h3 style='text-align: center;'>Вхід в AI Visibility</h3>", unsafe_allow_html=True)
        with st.form("login"):
            email = st.text_input("Email")
            password = st.text_input("Пароль", type="password")
            if st.form_submit_button("Увійти", use_container_width=True):
                role = "admin" if "admin" in email else "user"
                st.session_state['user'] = {"email": email}
                st.session_state['role'] = role
                st.session_state['user_details'] = {"first_name": "User", "last_name": ""}
                cookie_manager.set('virshi_token', f'mock_{role}', key="set_login")
                st.rerun()

def onboarding_wizard():
    st.markdown("## 🚀 Налаштування Проекту")
    with st.container(border=True):
        step = st.session_state.get('onboarding_step', 1)
        
        if step == 1:
            st.subheader("Крок 1: Про Вас")
            c1, c2 = st.columns(2)
            first = c1.text_input("Ім'я")
            last = c2.text_input("Прізвище")
            if st.button("Далі"):
                if first and last:
                    st.session_state['user_details'] = {"first_name": first, "last_name": last}
                    st.session_state['onboarding_step'] = 2
                    st.rerun()
                else: st.warning("Введіть дані")
        
        elif step == 2:
            st.subheader("Крок 2: Бренд")
            brand = st.text_input("Назва Бренду")
            domain = st.text_input("Домен")
            if st.button("Згенерувати запити"):
                if brand and domain:
                    st.session_state['temp_brand'] = brand
                    st.session_state['temp_domain'] = domain
                    # Mock N8N Call
                    with st.spinner("AI аналізує нішу та генерує запити..."):
                        prompts = mock_n8n_generate_prompts(brand, domain)
                        st.session_state['generated_prompts'] = prompts
                    st.session_state['onboarding_step'] = 3
                    st.rerun()
                else: st.warning("Введіть бренд і домен")
        
        elif step == 3:
            st.subheader("Крок 3: Оберіть 5 запитів")
            st.write(f"Ми підготували 10 запитів для **{st.session_state['temp_brand']}**. Оберіть 5 пріоритетних:")
            
            selected_prompts = st.multiselect(
                "Список запитів:", 
                st.session_state['generated_prompts'],
                default=st.session_state['generated_prompts'][:5]
            )
            
            st.caption(f"Обрано: {len(selected_prompts)} / 5")
            
            if st.button("Запустити Сканування"):
                if len(selected_prompts) == 5:
                    with st.spinner("Створення проекту та запуск демо-сканування..."):
                        time.sleep(2) # Fake processing
                        # Mock DB Creation
                        st.session_state['current_project'] = {
                            "name": st.session_state['temp_brand'],
                            "status": "trial",
                            "created_at": datetime.now().strftime("%Y-%m-%d"),
                            "id": "new-proj",
                            "keywords": selected_prompts
                        }
                    st.success("Готово!")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("Будь ласка, оберіть рівно 5 запитів.")

# --- 5. PAGE VIEWS ---

def show_dashboard():
    proj = st.session_state.get('current_project', {})
    
    # Header
    c_title, c_filter = st.columns([3, 1])
    with c_title: st.title(f"Дашборд: {proj.get('name', 'SkyUp')}")
    with c_filter: 
        time_range = st.selectbox("Період:", ["Останні 7 днів", "Останні 30 днів", "Останні 3 місяці"])
    
    st.markdown("---")
    
    # Get stats based on DB connection or Mock
    if DB_CONNECTED and proj.get('id'):
        stats = supabase.table('dashboard_stats').select("*").eq('project_id', proj['id']).execute().data
        stats = stats[0] if stats else {}
        sov = stats.get('sov', 0)
        off = stats.get('official_source_pct', 0)
        pos = stats.get('avg_position', 0)
    else:
        # Mock logic
        sov, off, pos = 30.86, 50.00, 1.2
        if time_range == "Останні 30 днів": sov += 2; off -= 5; pos = 1.4

    # KPI Grid
    c1, c2, c3 = st.columns(3)
    with c1:
        with st.container(border=True):
            st.markdown(f"**ЧАСТКА ГОЛОСУ (SOV)**", help=METRIC_TOOLTIPS["sov"])
            k, ch = st.columns([1, 1])
            k.markdown(f"## {sov:.2f}%")
            ch.plotly_chart(get_donut_chart(sov, "#00C896"), use_container_width=True)
    with c2:
        with st.container(border=True):
            st.markdown(f"**% ОФІЦІЙНИХ ДЖЕРЕЛ**", help=METRIC_TOOLTIPS["official"])
            k, ch = st.columns([1, 1])
            k.markdown(f"## {off:.2f}%")
            ch.plotly_chart(get_donut_chart(off, "#00C896"), use_container_width=True)
    with c3:
        with st.container(border=True):
            st.markdown(f"**ЗАГАЛЬНИЙ НАСТРІЙ**", help=METRIC_TOOLTIPS["sentiment"])
            # Static Pie for demo
            fig = go.Figure(data=[go.Pie(labels=['Pos','Neu','Neg'], values=[20,70,10], hole=0, marker_colors=['#00C896', '#9EA0A5', '#FF4B4B'])])
            fig.update_layout(height=80, margin=dict(t=0,b=0,l=0,r=0), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    c4, c5, c6 = st.columns(3)
    with c4:
        with st.container(border=True):
            st.markdown(f"**ПОЗИЦІЯ БРЕНДУ**", help=METRIC_TOOLTIPS["position"])
            st.markdown(f"<h1 style='text-align: center; color: #8041F6;'>{pos}</h1>", unsafe_allow_html=True)
            st.progress(int(100 - (pos*10)))
    with c5:
        with st.container(border=True):
            st.markdown(f"**ПРИСУТНІСТЬ БРЕНДУ**", help=METRIC_TOOLTIPS["presence"])
            k, ch = st.columns([1, 1])
            k.markdown(f"## 60.00%")
            ch.plotly_chart(get_donut_chart(60, "#00C896"), use_container_width=True)
    with c6:
        with st.container(border=True):
            st.markdown(f"**ЗГАДКИ ДОМЕНУ**", help=METRIC_TOOLTIPS["domain"])
            k, ch = st.columns([1, 1])
            k.markdown("## 10.00%")
            ch.plotly_chart(get_donut_chart(10, "#00C896"), use_container_width=True)

    # Chart
    st.markdown("### 📈 Динаміка Позицій")
    days = 7 if "7" in time_range else 30
    df_chart = pd.DataFrame({
        "Date": pd.date_range(end=datetime.today(), periods=days),
        "Position": [max(1, 3 + random.uniform(-1, 1)) for _ in range(days)]
    })
    fig = px.line(df_chart, x="Date", y="Position", template="plotly_white")
    fig.update_yaxes(autorange="reversed")
    st.plotly_chart(fig, use_container_width=True)

    # KEYWORDS LIST ON DASHBOARD
    st.markdown("### 📋 Моніторинг Запитів")
    
    # Fetch Keywords
    if DB_CONNECTED and proj.get('id'):
        kw_data = supabase.table('keywords').select("keyword_text").eq('project_id', proj['id']).execute().data
        keywords = [k['keyword_text'] for k in kw_data]
    else:
        # Fallback if created in onboarding or admin demo
        keywords = proj.get('keywords', ["дешеві авіаквитки", "skyup відгуки", "квитки київ варшава", "чартер єгипет", "правила багажу"])

    kw_df = pd.DataFrame({"Запит": keywords, "Статус": ["Active"]*len(keywords)})
    st.dataframe(kw_df, use_container_width=True, hide_index=True)

def show_admin():
    if st.session_state['role'] != 'admin': return
    st.title("🛡️ Super Admin Panel")
    
    # Fetch real projects if DB connected
    if DB_CONNECTED:
        try:
            projs = supabase.table('projects').select("*").execute().data
            df = pd.DataFrame(projs)
            st.dataframe(df, use_container_width=True)
        except: st.error("DB Error")
    else:
        st.info("Demo Data (DB not connected)")

def show_gpt_vis():
    st.title("🤖 GPT-Visibility Agent")
    st.info("Чат з базою даних...")

# --- 6. SIDEBAR ---

def sidebar_menu():
    with st.sidebar:
        # LOGO (Small & Compact)
        st.markdown('<div class="sidebar-logo">', unsafe_allow_html=True)
        st.image("https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png", width=160)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Admin Select
        if st.session_state['role'] == 'admin':
            st.markdown("### 🛠 Admin Select")
            if DB_CONNECTED:
                projs = supabase.table('projects').select("*").execute().data
                opts = {p['brand_name']: p for p in projs}
                sel = st.selectbox("Project", list(opts.keys()))
                if st.session_state.get('current_project', {}).get('name') != sel:
                    st.session_state['current_project'] = opts[sel]
            else:
                # Mock Admin List
                opts = ["SkyUp", "Monobank", "Nova Poshta", "Rozetka", "Ajax Systems"]
                sel = st.selectbox("Project", opts)
                if st.session_state.get('current_project', {}).get('name') != sel:
                    st.session_state['current_project'] = {
                        "name": sel, "status": "active" if sel != "SkyUp" else "trial", 
                        "id": "mock_id", "created_at": "2025-01-01"
                    }

        st.divider()

        # Project Info
        if st.session_state.get('current_project'):
            p = st.session_state['current_project']
            st.markdown(f"<div class='sidebar-label'>Current Brand</div>", unsafe_allow_html=True)
            badge = "<span class='badge-trial'>TRIAL</span>" if p.get('status') == 'trial' else "<span class='badge-active'>PRO</span>"
            st.markdown(f"**{p['name']}** {badge}", unsafe_allow_html=True)
            st.markdown(f"<div class='sidebar-label'>Created</div>", unsafe_allow_html=True)
            st.markdown(f"📅 {p.get('created_at', 'N/A')[:10]}")
            
            if p.get('status') == 'trial':
                st.markdown(f"""<a href="mailto:hi@virshi.ai?subject=Upgrade {p['name']}" class="upgrade-btn">⭐ Підвищити план</a>""", unsafe_allow_html=True)
            
            st.divider()

        # Menu - GPT-Visibility Last!
        opts = ["Дашборд", "Перелік запитів", "Джерела", "Конкуренти", "Рекомендації"]
        icons = ["speedometer2", "list-ul", "hdd-network", "people", "lightbulb"]
        
        # GPT Visibility is Last
        opts.append("GPT-Visibility")
        icons.append("robot")

        if st.session_state['role'] == 'admin':
            opts.append("Адмін")
            icons.append("shield-lock")

        selected = option_menu(
            menu_title=None, options=opts, icons=icons,
            menu_icon="cast", default_index=0,
            styles={"nav-link-selected": {"background-color": "#8041F6"}, "container": {"padding": "0!important"}}
        )
        
        st.divider()
        
        # User Info
        if st.session_state['user']:
            det = st.session_state.get('user_details', {})
            full = f"{det.get('first_name','')} {det.get('last_name','')}"
            st.markdown(f"<div class='sidebar-name'>{full}</div>", unsafe_allow_html=True)
            if st.session_state['role'] == 'admin': st.caption("🔴 SUPER ADMIN")
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
                st.session_state['user']=None; cookie_manager.delete('virshi_token'); st.rerun()
        onboarding_wizard()
    else:
        # Ensure admin has a project selected
        if st.session_state['role'] == 'admin' and not st.session_state.get('current_project'):
             # Trigger sidebar execution to select default
             pass 

        page = sidebar_menu()
        
        if page == "Дашборд": show_dashboard()
        elif page == "Перелік запитів": 
            st.title("📋 Перелік запитів")
            proj = st.session_state.get('current_project', {})
            if DB_CONNECTED:
                kw = supabase.table('keywords').select("*").eq('project_id', proj.get('id')).execute().data
                st.dataframe(pd.DataFrame(kw), use_container_width=True)
            else:
                st.info("Demo keywords list...")
        elif page == "Джерела": st.title("📡 Джерела")
        elif page == "Конкуренти": st.title("⚔️ Конкуренти")
        elif page == "Рекомендації": st.title("💡 Рекомендації")
        elif page == "GPT-Visibility": show_gpt_vis()
        elif page == "Адмін": show_admin()

if __name__ == "__main__":
    main()
