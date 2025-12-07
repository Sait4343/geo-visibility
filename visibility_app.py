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
    
    /* Sidebar Compact */
    section[data-testid="stSidebar"] { background-color: #FFFFFF; border-right: 1px solid #E0E0E0; }
    section[data-testid="stSidebar"] > div:first-child { padding-top: 0.5rem; }
    .sidebar-logo { margin-bottom: 5px; text-align: center; }
    
    /* Card Style */
    .css-1r6slb0, .css-12oz5g7, div[data-testid="stForm"] { 
        background-color: white; padding: 20px; border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05); border: 1px solid #EAEAEA;
    }
    div[data-testid="stMetric"] {
        background-color: #ffffff; border: 1px solid #e0e0e0; padding: 15px;
        border-radius: 10px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    
    /* Buttons */
    .stButton>button { background-color: #8041F6; color: white; border-radius: 8px; border: none; height: 45px; font-weight: 600;}
    .stButton>button:hover { background-color: #6a35cc; }
    
    /* Yellow Upgrade Button */
    .upgrade-btn {
        display: block; width: 100%; background-color: #FFC107; color: #000000;
        text-align: center; padding: 10px; border-radius: 8px;
        text-decoration: none; font-weight: bold; margin-top: 10px; border: 1px solid #e0a800;
    }
    .upgrade-btn:hover { background-color: #e0a800; color: #000000; }

    /* Badges */
    .badge-trial { background-color: #FFECB3; color: #856404; padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 0.7em; }
    .badge-active { background-color: #D4EDDA; color: #155724; padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 0.7em; }
    
    .sidebar-name { font-size: 14px; font-weight: 600; color: #333; margin-top: 5px;}
    .sidebar-label { font-size: 11px; color: #999; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 15px;}
</style>
""", unsafe_allow_html=True)

# --- 2. SETUP ---

# ✅ ВИПРАВЛЕНО: Ініціалізація без кешування
cookie_manager = stx.CookieManager()

# Initialize Supabase
try:
    SUPABASE_URL = st.secrets.get("SUPABASE_URL", {}).get("url", "https://placeholder.supabase.co")
    SUPABASE_KEY = st.secrets.get("SUPABASE_URL", {}).get("key", "placeholder")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    DB_CONNECTED = True if "placeholder" not in SUPABASE_URL else False
except:
    DB_CONNECTED = False

# Session State
if 'user' not in st.session_state: st.session_state['user'] = None
if 'user_details' not in st.session_state: st.session_state['user_details'] = {}
if 'role' not in st.session_state: st.session_state['role'] = 'user'
if 'current_project' not in st.session_state: st.session_state['current_project'] = None
if 'gpt_history' not in st.session_state: st.session_state['gpt_history'] = []
if 'generated_prompts' not in st.session_state: st.session_state['generated_prompts'] = []

# --- 3. HELPER FUNCTIONS ---

def call_n8n_generate_prompts(brand, domain):
    """Виклик реального вебхука n8n або мок-дані"""
    webhook_url = st.secrets.get("N8N", {}).get("webhook_url")
    
    if webhook_url:
        try:
            response = requests.post(webhook_url, json={"brand": brand, "domain": domain}, timeout=10)
            if response.status_code == 200:
                data = response.json()
                # n8n має повертати JSON: { "prompts": ["p1", "p2"...] }
                return data.get("prompts", [])
        except Exception as e:
            st.error(f"Помилка n8n: {e}")
            
    # Fallback (Mock) if no URL or error
    time.sleep(1.5) 
    return [
        f"Які авіакомпанії пропонують найкращі ціни на {domain}?",
        f"Відгуки про {brand} 2025",
        f"Як замовити послуги {brand} онлайн?",
        f"Акції та знижки {brand} цього місяця",
        f"Порівняння {brand} з конкурентами",
        f"Чи надійна компанія {brand}?",
        f"Контакти підтримки {domain}",
        f"Мобільний додаток {brand} огляд",
        f"Історія успіху {brand}",
        f"Чому обирають {brand}?"
    ]

def get_donut_chart(value, color="#00C896"):
    remaining = max(0, 100 - value)
    fig = go.Figure(data=[go.Pie(
        values=[value, remaining], hole=.75,
        marker_colors=[color, '#F0F2F6'], textinfo='none', hoverinfo='label+percent'
    )])
    fig.update_layout(
        showlegend=False, margin=dict(t=0, b=0, l=0, r=0), height=80, width=80,
        annotations=[dict(text=f"{int(value)}%", x=0.5, y=0.5, font_size=14, showarrow=False, font_weight="bold", font_color="#333")]
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
                    r, d = get_user_role_and_details(res.user.id)
                    st.session_state['role'] = r
                    st.session_state['user_details'] = d
            except: cookie_manager.delete('virshi_auth_token')
        elif token == 'mock_admin_token' and not DB_CONNECTED:
            st.session_state['user'] = {"email": "admin@virshi.ai"}
            st.session_state['role'] = "admin"
            st.session_state['user_details'] = {"first_name": "Super", "last_name": "Admin"}

def login_user(email, password):
    if DB_CONNECTED:
        try:
            res = supabase.auth.sign_in_with_password({"email": email, "password": password})
            st.session_state['user'] = res.user
            cookie_manager.set('virshi_auth_token', res.session.access_token, expires_at=datetime.now() + timedelta(days=7))
            r, d = get_user_role_and_details(res.user.id)
            st.session_state['role'] = r
            st.session_state['user_details'] = d
            return True
        except Exception as e:
            st.error(f"Помилка: {e}")
            return False
    else:
        # Mock
        role = "admin" if "admin" in email else "user"
        st.session_state['user'] = {"email": email}
        st.session_state['role'] = role
        st.session_state['user_details'] = {"first_name": "Demo", "last_name": "User"}
        cookie_manager.set('virshi_auth_token', f'mock_{role}_token', key="set_mock")
        return True

def register_user(email, password, f_name, l_name):
    if DB_CONNECTED:
        try:
            res = supabase.auth.sign_up({
                "email": email, "password": password,
                "options": {"data": {"first_name": f_name, "last_name": l_name}}
            })
            if res.user:
                # Manual profile creation (if trigger not set)
                supabase.table('profiles').insert({
                    "id": res.user.id, "email": email,
                    "first_name": f_name, "last_name": l_name, "role": "user"
                }).execute()
                st.success("Успішно! Увійдіть.")
                return True
        except Exception as e:
            st.error(f"Помилка: {e}")
            return False
    return True

def logout():
    supabase.auth.sign_out() if DB_CONNECTED else None
    cookie_manager.delete('virshi_auth_token')
    st.session_state['user'] = None
    st.session_state['current_project'] = None
    st.rerun()

# --- 5. UI: LOGIN ---

def login_page():
    col_l, col_c, col_r = st.columns([1, 1.5, 1])
    with col_c:
        # Centered Logo
        c1, c2, c3 = st.columns([1, 2, 1])
        with c2: st.image("https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png", use_container_width=True)
        st.markdown("<br>", unsafe_allow_html=True)

        tab1, tab2 = st.tabs(["🔑 Вхід", "📝 Реєстрація"])
        
        with tab1:
            with st.form("login_form"):
                email = st.text_input("Емейл")
                password = st.text_input("Пароль", type="password")
                if st.form_submit_button("Увійти", use_container_width=True):
                    if login_user(email, password): st.rerun()
        
        with tab2:
            with st.form("reg_form"):
                email = st.text_input("Емейл")
                pas = st.text_input("Пароль", type="password")
                c_a, c_b = st.columns(2)
                f = c_a.text_input("Ім'я")
                l = c_b.text_input("Прізвище")
                if st.form_submit_button("Зареєструватися", use_container_width=True):
                    if email and pas and f: register_user(email, pas, f, l)
                    else: st.warning("Заповніть поля")

# --- 6. UI: ONBOARDING ---

def onboarding_wizard():
    st.markdown("## 🚀 Налаштування Проекту")
    with st.container(border=True):
        step = st.session_state.get('onboarding_step', 2) # Assuming name is known or step 1 skipped if reg
        
        # Step 1: Name (only if missing)
        if not st.session_state.get('user_details', {}).get('first_name'):
             st.subheader("Давайте знайомитись")
             f = st.text_input("Ваше ім'я")
             if st.button("Далі"):
                 st.session_state['user_details']['first_name'] = f
                 st.rerun()
                 
        elif step == 2:
            st.subheader("Крок 1: Бренд та Домен")
            brand = st.text_input("Назва Бренду")
            domain = st.text_input("Домен (напр. site.com)")
            if st.button("Згенерувати запити"):
                if brand and domain:
                    st.session_state['temp_brand'] = brand
                    st.session_state['temp_domain'] = domain
                    with st.spinner("AI генерує промпти..."):
                        prompts = call_n8n_generate_prompts(brand, domain)
                        st.session_state['generated_prompts'] = prompts
                    st.session_state['onboarding_step'] = 3
                    st.rerun()
                else: st.warning("Заповніть поля")
        
        elif step == 3:
            st.subheader("Крок 2: Оберіть 5 запитів")
            st.write(f"Оберіть 5 найважливіших для **{st.session_state['temp_brand']}**:")
            
            selected = st.multiselect("Список запитів:", st.session_state['generated_prompts'], default=st.session_state['generated_prompts'][:5])
            st.caption(f"Обрано: {len(selected)} / 5")
            
            if st.button("Створити Проект"):
                if len(selected) == 5:
                    with st.spinner("Налаштування дашборду..."):
                        # Insert into DB (Mock logic or Real)
                        # In real app: insert into projects table, then insert keywords
                        time.sleep(1)
                        st.session_state['current_project'] = {
                            "id": "new_id",
                            "name": st.session_state['temp_brand'],
                            "status": "trial",
                            "created_at": datetime.now().strftime("%Y-%m-%d"),
                            "keywords": selected
                        }
                    st.success("Готово!")
                    time.sleep(0.5)
                    st.rerun()
                else: st.error("Оберіть рівно 5 запитів")

# --- 7. DASHBOARD ---

def show_dashboard():
    proj = st.session_state.get('current_project', {})
    
    # 1. Header
    c_title, c_filt = st.columns([3, 1])
    with c_title: st.title(f"Дашборд: {proj.get('name')}")
    with c_filt: tr = st.selectbox("Період:", ["Останні 7 днів", "Останні 30 днів"])
    st.markdown("---")
    
    # 2. Stats
    if DB_CONNECTED and proj.get('id'):
        stats = supabase.table('dashboard_stats').select("*").eq('project_id', proj['id']).execute().data
        s = stats[0] if stats else {}
        sov, off, pos = s.get('sov', 0), s.get('official_source_pct', 0), s.get('avg_position', 0)
    else:
        sov, off, pos = 30.86, 50.00, 1.2
        if "30" in tr: sov += 2
        
    # KPI Grid
    c1, c2, c3 = st.columns(3)
    with c1:
        with st.container(border=True):
            st.markdown("**ЧАСТКА ГОЛОСУ (SOV)**", help=METRIC_TOOLTIPS["sov"])
            k, ch = st.columns([1, 1])
            k.markdown(f"## {sov:.2f}%")
            ch.plotly_chart(get_donut_chart(sov), use_container_width=True)
    with c2:
        with st.container(border=True):
            st.markdown("**% ОФІЦІЙНИХ ДЖЕРЕЛ**", help=METRIC_TOOLTIPS["official"])
            k, ch = st.columns([1, 1])
            k.markdown(f"## {off:.2f}%")
            ch.plotly_chart(get_donut_chart(off), use_container_width=True)
    with c3:
        with st.container(border=True):
            st.markdown("**ЗАГАЛЬНИЙ НАСТРІЙ**", help=METRIC_TOOLTIPS["sentiment"])
            # Static Pie
            fig = go.Figure(data=[go.Pie(labels=['Pos','Neu','Neg'], values=[20,70,10], hole=0, marker_colors=['#00C896', '#9EA0A5', '#FF4B4B'])])
            fig.update_layout(height=80, margin=dict(t=0,b=0,l=0,r=0), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
            
    c4, c5, c6 = st.columns(3)
    with c4:
        with st.container(border=True):
            st.markdown("**ПОЗИЦІЯ БРЕНДУ**", help=METRIC_TOOLTIPS["position"])
            st.markdown(f"<h1 style='text-align: center; color: #8041F6;'>{pos}</h1>", unsafe_allow_html=True)
            st.progress(int(100 - pos*10))
    with c5:
        with st.container(border=True):
            st.markdown("**ПРИСУТНІСТЬ БРЕНДУ**", help=METRIC_TOOLTIPS["presence"])
            k, ch = st.columns([1, 1])
            k.markdown(f"## 60.0%")
            ch.plotly_chart(get_donut_chart(60), use_container_width=True)
    with c6:
        with st.container(border=True):
            st.markdown("**ЗГАДКИ ДОМЕНУ**", help=METRIC_TOOLTIPS["domain"])
            k, ch = st.columns([1, 1])
            k.markdown(f"## 10.0%")
            ch.plotly_chart(get_donut_chart(10), use_container_width=True)

    # 3. Chart
    st.markdown("### 📈 Динаміка Позицій")
    days = 7 if "7" in tr else 30
    df = pd.DataFrame({"Date": pd.date_range(end=datetime.today(), periods=days), "Pos": [max(1, 3+random.uniform(-1,1)) for _ in range(days)]})
    fig = px.line(df, x="Date", y="Pos", template="plotly_white")
    fig.update_yaxes(autorange="reversed")
    st.plotly_chart(fig, use_container_width=True)
    
    # 4. Keyword Table (NEW)
    st.markdown("### 📋 Перелік запитів")
    if DB_CONNECTED and proj.get('id'):
        data = supabase.table('keywords').select("keyword_text").eq('project_id', proj['id']).execute().data
        kws = [d['keyword_text'] for d in data]
    else:
        kws = proj.get('keywords', ["Demo query 1", "Demo query 2"])
    
    st.dataframe(pd.DataFrame({"Запит": kws, "Статус": ["Active"]*len(kws)}), use_container_width=True, hide_index=True)

def show_admin():
    if st.session_state['role'] != 'admin': return
    st.title("🛡️ Super Admin Panel")
    if DB_CONNECTED:
        try:
            d = supabase.table('projects').select("*").execute().data
            st.dataframe(pd.DataFrame(d), use_container_width=True)
        except: st.error("DB Error")
    else: st.info("Demo Admin View")

def show_gpt_vis():
    st.title("🤖 GPT-Visibility")
    st.info("Чат з агентом...")

# --- 8. SIDEBAR ---

def sidebar_menu():
    with st.sidebar:
        # Smaller Logo
        st.markdown('<div class="sidebar-logo">', unsafe_allow_html=True)
        st.image("https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png", width=120)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Admin Select
        if st.session_state['role'] == 'admin':
            st.markdown("### 🛠 Admin Select")
            if DB_CONNECTED:
                projs = supabase.table('projects').select("*").execute().data
                opts = {p['brand_name']: p for p in projs}
                if opts:
                    sel = st.selectbox("Project", list(opts.keys()))
                    if st.session_state.get('current_project', {}).get('name') != sel:
                        st.session_state['current_project'] = opts[sel]
                        st.rerun()
            else:
                opts = ["SkyUp", "Monobank", "Nova Poshta", "Rozetka", "Ajax Systems"]
                sel = st.selectbox("Project", opts)
                if st.session_state.get('current_project', {}).get('name') != sel:
                    st.session_state['current_project'] = {"name": sel, "status": "active", "id": "m", "created_at": "2025-01-01"}
                    st.rerun()
        st.divider()

        # Project Info
        if st.session_state.get('current_project'):
            p = st.session_state['current_project']
            st.markdown(f"<div class='sidebar-label'>Current Brand</div>", unsafe_allow_html=True)
            
            badge = "<span class='badge-trial'>TRIAL</span>" if p.get('status') == 'trial' else "<span class='badge-active'>PRO</span>"
            st.markdown(f"**{p['name']}** {badge}", unsafe_allow_html=True)
            
            st.markdown(f"<div class='sidebar-label'>Created</div>", unsafe_allow_html=True)
            st.markdown(f"📅 {str(p.get('created_at', 'N/A'))[:10]}")
            
            if p.get('status') == 'trial':
                st.markdown(f"""<a href="mailto:hi@virshi.ai?subject=Upgrade {p['name']}" class="upgrade-btn">⭐ Підвищити план</a>""", unsafe_allow_html=True)
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
        
        # User
        if st.session_state['user']:
            det = st.session_state.get('user_details', {})
            full = f"{det.get('first_name','')} {det.get('last_name','')}"
            st.markdown(f"<div class='sidebar-name'>{full}</div>", unsafe_allow_html=True)
            if st.session_state['role']=='admin': st.caption("🔴 SUPER ADMIN")
            st.markdown("**Support:** [hi@virshi.ai](mailto:hi@virshi.ai)")
            if st.button("Вийти"): logout()
            
    return selected

# --- 9. MAIN ---

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
             st.session_state['current_project'] = {"name": "Select...", "status": "active"} 

        page = sidebar_menu()
        if page == "Дашборд": show_dashboard()
        elif page == "Перелік запитів":
            st.title("📋 Перелік запитів")
            p = st.session_state.get('current_project', {})
            if DB_CONNECTED and p.get('id'):
                data = supabase.table('keywords').select("*").eq('project_id', p['id']).execute().data
                st.dataframe(pd.DataFrame(data), use_container_width=True)
            else: st.info("Demo list...")
        elif page == "Джерела": st.title("📡 Джерела")
        elif page == "Конкуренти": st.title("⚔️ Конкуренти")
        elif page == "Рекомендації": st.title("💡 Рекомендації")
        elif page == "GPT-Visibility": show_gpt_vis()
        elif page == "Адмін": show_admin()

if __name__ == "__main__":
    main()
