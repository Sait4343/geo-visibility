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
    /* Global Background */
    .stApp { background-color: #F4F6F9; }
    
    /* Sidebar Styling */
    section[data-testid="stSidebar"] { background-color: #FFFFFF; border-right: 1px solid #E0E0E0; }
    
    /* Logo sizing & placement */
    section[data-testid="stSidebar"] > div:first-child { padding-top: 0.5rem; }
    .sidebar-logo-container { display: flex; justify-content: center; margin-bottom: 10px; }
    .sidebar-logo-container img { width: 140px; }
    
    /* Cards (White containers) */
    .css-1r6slb0, .css-12oz5g7, div[data-testid="stForm"] { 
        background-color: white; 
        padding: 20px; 
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05); 
        border: 1px solid #EAEAEA;
    }
    
    /* Metrics */
    div[data-testid="stMetric"] {
        background-color: #ffffff; border: 1px solid #e0e0e0; padding: 15px;
        border-radius: 10px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    
    /* Buttons */
    .stButton>button { background-color: #8041F6; color: white; border-radius: 8px; border: none; font-weight: 600; }
    .stButton>button:hover { background-color: #6a35cc; }
    
    /* Upgrade Button (Yellow) */
    .upgrade-btn {
        display: block; width: 100%; background-color: #FFC107; color: #000000;
        text-align: center; padding: 8px; border-radius: 8px;
        text-decoration: none; font-weight: bold; margin-top: 10px; border: 1px solid #e0a800;
    }
    .upgrade-btn:hover { background-color: #e0a800; color: #000000; }

    /* Badges & Text */
    .badge-trial { background-color: #FFECB3; color: #856404; padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 0.7em; }
    .badge-active { background-color: #D4EDDA; color: #155724; padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 0.7em; }
    .sidebar-name { font-size: 14px; font-weight: 600; color: #333; margin-top: 5px;}
    .sidebar-label { font-size: 11px; color: #999; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 15px;}
</style>
""", unsafe_allow_html=True)

# --- 2. SETUP & CONNECTION ---

cookie_manager = stx.CookieManager()

# N8N Webhook URL (Replace with your actual URL)
N8N_WEBHOOK_URL = "https://your-n8n-instance.com/webhook/generate-prompts"

# Initialize Supabase
try:
    SUPABASE_URL = st.secrets.get("SUPABASE_URL", {}).get("url", "https://placeholder.supabase.co")
    SUPABASE_KEY = st.secrets.get("SUPABASE_URL", {}).get("key", "placeholder")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    if "placeholder" in SUPABASE_URL: DB_CONNECTED = False
    else: DB_CONNECTED = True
except Exception: DB_CONNECTED = False

# Session State Init
if 'user' not in st.session_state: st.session_state['user'] = None
if 'user_details' not in st.session_state: st.session_state['user_details'] = {} 
if 'role' not in st.session_state: st.session_state['role'] = 'user'
if 'current_project' not in st.session_state: st.session_state['current_project'] = None
if 'gpt_history' not in st.session_state: st.session_state['gpt_history'] = []
if 'generated_prompts' not in st.session_state: st.session_state['generated_prompts'] = []

# --- 3. HELPER FUNCTIONS ---

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

def generate_prompts_via_n8n(brand, domain):
    """
    1. Відправляє запит на N8N. 
    2. Якщо N8N недоступний, використовує локальний генератор.
    """
    try:
        # payload = {"brand": brand, "domain": domain}
        # response = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=5)
        # return response.json().get('prompts', [])
        raise Exception("N8N not configured yet") # Remove this line when N8N is ready
    except:
        # Fallback Generator
        time.sleep(1.5)
        return [
            f"Які {brand} пропонують найкращі умови на {domain}?",
            f"Відгуки про {brand} 2025",
            f"Як замовити послуги {brand} онлайн?",
            f"Порівняння цін {brand} та конкурентів",
            f"Чи надійна компанія {brand}?",
            f"Контакти підтримки {domain}",
            f"Огляд сервісу {brand}",
            f"Акції та знижки {brand}",
            f"Історія бренду {brand}",
            f"Переваги та недоліки {brand}"
        ]

def simulate_initial_analysis(project_id, keywords):
    """
    Створює фейкові дані аналізу в базі, щоб користувач одразу бачив результат.
    Це імітує роботу N8N, який би проаналізував ці 5 запитів.
    """
    if not DB_CONNECTED: return
    
    try:
        # 1. Створюємо Scan Run
        scan_res = supabase.table('scan_runs').insert({"project_id": project_id, "provider": "gemini"}).execute()
        scan_id = scan_res.data[0]['id']
        
        # 2. Додаємо результат для кожного слова (спрощено один запис на скан)
        supabase.table('brand_mentions').insert({
            "scan_run_id": scan_id, "brand_name": "MyBrand", 
            "is_my_brand": True, "rank_position": 1, "sentiment_score": 85
        }).execute()
        
        # 3. Додаємо джерело
        supabase.table('extracted_sources').insert({
            "scan_run_id": scan_id, "domain": "mywebsite.com", "is_official": True
        }).execute()
    except Exception as e:
        st.error(f"Error simulating data: {e}")

# --- 4. AUTHENTICATION ---

def get_user_role_and_details(user_id):
    if DB_CONNECTED:
        try:
            data = supabase.table('profiles').select("*").eq('id', user_id).execute()
            if data.data:
                p = data.data[0]
                return p.get('role', 'user'), {"first_name": p.get('first_name'), "last_name": p.get('last_name')}
        except: pass
    return 'user', {}

def check_session():
    if st.session_state['user'] is None:
        time.sleep(0.1)
        token = cookie_manager.get('virshi_auth_token')
        if token and DB_CONNECTED:
            try:
                res = supabase.auth.get_user(token)
                if res.user:
                    st.session_state['user'] = res.user
                    role, details = get_user_role_and_details(res.user.id)
                    st.session_state['role'] = role
                    st.session_state['user_details'] = details
            except: cookie_manager.delete('virshi_auth_token')
        
        # Mock logic
        elif token == 'mock_admin':
            st.session_state['user'] = {"email": "admin@virshi.ai", "id": "m1"}
            st.session_state['role'] = "admin"
            st.session_state['user_details'] = {"first_name": "Super", "last_name": "Admin"}

def login_page():
    col_l, col_center, col_r = st.columns([1, 1.5, 1])
    with col_center:
        st.markdown('<div style="text-align: center;"><img src="https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png" width="180"></div>', unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        
        tab_login, tab_reg = st.tabs(["🔑 Вхід", "📝 Реєстрація"])
        
        with tab_login:
            with st.form("login"):
                email = st.text_input("Емейл")
                password = st.text_input("Пароль", type="password")
                if st.form_submit_button("Увійти", use_container_width=True):
                    if DB_CONNECTED:
                        try:
                            res = supabase.auth.sign_in_with_password({"email": email, "password": password})
                            st.session_state['user'] = res.user
                            cookie_manager.set('virshi_auth_token', res.session.access_token)
                            role, det = get_user_role_and_details(res.user.id)
                            st.session_state['role'] = role; st.session_state['user_details'] = det
                            st.rerun()
                        except: st.error("Невірні дані")
                    else:
                        role = "admin" if "admin" in email else "user"
                        st.session_state['user'] = {"email": email}
                        st.session_state['role'] = role
                        cookie_manager.set('virshi_auth_token', 'mock_admin' if role=='admin' else 'mock_u')
                        st.rerun()

        with tab_reg:
            with st.form("reg"):
                new_email = st.text_input("Емейл")
                new_pass = st.text_input("Пароль", type="password")
                c1, c2 = st.columns(2)
                fn = c1.text_input("Ім'я"); ln = c2.text_input("Прізвище")
                if st.form_submit_button("Зареєструватися", use_container_width=True):
                    if DB_CONNECTED:
                        try:
                            res = supabase.auth.sign_up({"email": new_email, "password": new_pass, "options": {"data": {"first_name": fn}}})
                            if res.user:
                                supabase.table('profiles').insert({"id": res.user.id, "email": new_email, "first_name": fn, "last_name": ln}).execute()
                                st.success("Успішно! Увійдіть.")
                        except Exception as e: st.error(f"Помилка: {e}")

def logout():
    supabase.auth.sign_out() if DB_CONNECTED else None
    cookie_manager.delete('virshi_auth_token')
    st.session_state['user'] = None
    st.session_state['current_project'] = None
    st.rerun()

# --- 5. ONBOARDING ---

def onboarding_wizard():
    st.markdown("## 🚀 Налаштування Проекту")
    
    with st.container(border=True):
        step = st.session_state.get('onboarding_step', 2)
        
        if not st.session_state.get('user_details', {}).get('first_name'):
             st.subheader("Давайте знайомитись")
             f = st.text_input("Ваше ім'я")
             if st.button("Далі"):
                 st.session_state['user_details'] = {"first_name": f}
                 st.rerun()
                 
        elif step == 2:
            st.subheader("Крок 1: Бренд та Домен")
            brand = st.text_input("Назва Бренду")
            domain = st.text_input("Домен")
            if st.button("Згенерувати запити"):
                if brand and domain:
                    st.session_state['temp_brand'] = brand
                    st.session_state['temp_domain'] = domain
                    with st.spinner("AI аналізує нішу..."):
                        prompts = generate_prompts_via_n8n(brand, domain)
                        st.session_state['generated_prompts'] = prompts
                    st.session_state['onboarding_step'] = 3
                    st.rerun()
                else: st.warning("Заповніть поля")
        
        elif step == 3:
            st.subheader("Крок 2: Оберіть 5 запитів")
            st.write(f"Ми знайшли 10 актуальних запитів для **{st.session_state['temp_brand']}**.")
            
            selected = st.multiselect("Оберіть 5 пріоритетних:", st.session_state['generated_prompts'], default=st.session_state['generated_prompts'][:5])
            st.caption(f"Обрано: {len(selected)} / 5")
            
            if st.button("Запустити Сканування"):
                if len(selected) == 5:
                    with st.spinner("Створення проекту та аналіз (Gemini)..."):
                        if DB_CONNECTED:
                            # 1. Create Project
                            user_id = st.session_state['user'].id
                            res = supabase.table('projects').insert({
                                "user_id": user_id, 
                                "brand_name": st.session_state['temp_brand'], 
                                "domain": st.session_state['temp_domain'],
                                "status": "trial"
                            }).execute()
                            proj_id = res.data[0]['id']
                            
                            # 2. Insert Keywords
                            for kw in selected:
                                supabase.table('keywords').insert({"project_id": proj_id, "keyword_text": kw}).execute()
                            
                            # 3. Simulate Analysis Results (Generate Data)
                            simulate_initial_analysis(proj_id, selected)
                            
                            # 4. Set Session
                            st.session_state['current_project'] = res.data[0]
                        else:
                            time.sleep(2)
                            st.session_state['current_project'] = {"id": "mock", "name": st.session_state['temp_brand'], "status": "trial", "created_at": "2025-01-01", "keywords": selected}
                            
                    st.success("Готово!")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("Оберіть рівно 5 запитів")

# --- 6. MAIN DASHBOARD ---

def show_dashboard():
    proj = st.session_state.get('current_project', {})
    
    # Header
    c1, c2 = st.columns([3, 1])
    with c1: st.title(f"Дашборд: {proj.get('name', 'My Brand')}")
    with c2: time_range = st.selectbox("Період:", ["Останні 7 днів", "Останні 30 днів", "Останні 3 місяці"])
    st.markdown("---")
    
    # Fetch Stats
    sov, off, pos = 0, 0, 0
    if DB_CONNECTED and proj.get('id'):
        stats = supabase.table('dashboard_stats').select("*").eq('project_id', proj['id']).execute().data
        if stats:
            s = stats[0]
            sov, off, pos = s['sov'], s['official_source_pct'], s['avg_position']
    
    # KPI Grid
    k1, k2, k3 = st.columns(3)
    with k1:
        with st.container(border=True):
            st.markdown(f"**ЧАСТКА ГОЛОСУ (SOV)**", help=METRIC_TOOLTIPS["sov"])
            c, ch = st.columns([1,1])
            c.markdown(f"## {sov}%")
            ch.plotly_chart(get_donut_chart(sov), use_container_width=True)
    with k2:
        with st.container(border=True):
            st.markdown(f"**% ОФІЦІЙНИХ ДЖЕРЕЛ**", help=METRIC_TOOLTIPS["official"])
            c, ch = st.columns([1,1])
            c.markdown(f"## {off}%")
            ch.plotly_chart(get_donut_chart(off), use_container_width=True)
    with k3:
        with st.container(border=True):
            st.markdown(f"**ЗАГАЛЬНИЙ НАСТРІЙ**", help=METRIC_TOOLTIPS["sentiment"])
            fig = go.Figure(data=[go.Pie(labels=['Pos','Neu','Neg'], values=[60,30,10], hole=0, marker_colors=['#00C896', '#9EA0A5', '#FF4B4B'])])
            fig.update_layout(height=80, margin=dict(t=0,b=0,l=0,r=0), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
            
    # Keywords Table
    st.markdown("### 📋 Перелік запитів")
    if DB_CONNECTED and proj.get('id'):
        kws = supabase.table('keywords').select("keyword_text").eq('project_id', proj['id']).execute().data
        data = [{"Запит": k['keyword_text'], "Статус": "Active"} for k in kws]
    else:
        data = [{"Запит": k, "Статус": "Active"} for k in proj.get('keywords', [])]
    
    st.dataframe(pd.DataFrame(data), use_container_width=True, hide_index=True)

# --- 7. SIDEBAR ---

def sidebar_menu():
    with st.sidebar:
        st.markdown('<div class="sidebar-logo-container"><img src="https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png"></div>', unsafe_allow_html=True)
        
        # Admin Selector
        if st.session_state['role'] == 'admin':
            st.markdown("### 🛠 Admin Select")
            if DB_CONNECTED:
                projs = supabase.table('projects').select("*").execute().data
                opts = {p['brand_name']: p for p in projs}
                sel = st.selectbox("Project", list(opts.keys()))
                if st.session_state.get('current_project', {}).get('name') != sel:
                    st.session_state['current_project'] = opts[sel]
                    st.rerun()
            else:
                opts = ["SkyUp", "Monobank"]
                sel = st.selectbox("Project", opts)
                if st.session_state.get('current_project', {}).get('name') != sel:
                    st.session_state['current_project'] = {"name": sel, "status": "active", "id": "m"}
                    st.rerun()
        
        st.divider()
        
        # Project Info
        if st.session_state.get('current_project'):
            p = st.session_state['current_project']
            st.markdown(f"<div class='sidebar-label'>Current Brand</div>", unsafe_allow_html=True)
            badge = "<span class='badge-trial'>TRIAL</span>" if p.get('status') == 'trial' else "<span class='badge-active'>PRO</span>"
            st.markdown(f"**{p.get('brand_name') or p.get('name')}** {badge}", unsafe_allow_html=True)
            st.markdown(f"<div class='sidebar-label'>Created</div>", unsafe_allow_html=True)
            st.markdown(f"📅 {p.get('created_at', 'N/A')[:10]}")
            
            if p.get('status') == 'trial':
                st.markdown(f"""<a href="mailto:hi@virshi.ai" class="upgrade-btn">⭐ Підвищити план</a>""", unsafe_allow_html=True)
            st.divider()

        # Menu
        opts = ["Дашборд", "Перелік запитів", "Джерела", "Конкуренти", "Рекомендації", "GPT-Visibility"]
        icons = ["speedometer2", "list-ul", "hdd-network", "people", "lightbulb", "robot"]
        
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
            d = st.session_state.get('user_details', {})
            full = f"{d.get('first_name','')} {d.get('last_name','')}"
            st.markdown(f"<div class='sidebar-name'>{full}</div>", unsafe_allow_html=True)
            st.markdown("**Support:** [hi@virshi.ai](mailto:hi@virshi.ai)")
            if st.button("Вийти"): logout()
            
    return selected

# --- 8. ROUTER ---

def main():
    check_session()
    
    if not st.session_state['user']:
        login_page()
    elif st.session_state.get('current_project') is None and st.session_state['role'] != 'admin':
        with st.sidebar:
            if st.button("Вийти"): logout()
        onboarding_wizard()
    else:
        # Default for admin
        if st.session_state['role'] == 'admin' and not st.session_state.get('current_project'):
             pass 

        page = sidebar_menu()
        
        if page == "Дашборд": show_dashboard()
        elif page == "Перелік запитів": 
            st.title("📋 Перелік запитів")
            # Logic duplicated in dashboard, simplifies here
            show_dashboard() 
        elif page == "Джерела": st.title("📡 Джерела")
        elif page == "Конкуренти": st.title("⚔️ Конкуренти")
        elif page == "Рекомендації": st.title("💡 Рекомендації")
        elif page == "GPT-Visibility": st.title("🤖 GPT-Visibility"); st.info("Chat...")
        elif page == "Адмін": 
            st.title("🛡️ Admin Panel")
            if DB_CONNECTED:
                st.dataframe(pd.DataFrame(supabase.table('projects').select("*").execute().data))

if __name__ == "__main__":
    main()
