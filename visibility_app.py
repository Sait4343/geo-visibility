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

# --- 1. CONFIGURATION ---
st.set_page_config(
    page_title="AI Visibility by Virshi",
    page_icon="👁️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 2. CONSTANTS & N8N WEBHOOKS ---
# 🔴 ЗАМІНІТЬ ЦІ URL НА ВАШІ РЕАЛЬНІ З N8N
N8N_GEN_URL = "https://virshi.app.n8n.cloud/webhook-test/6f8df20a-0c54-4ac9-8410-796a86786938" 
N8N_ANALYZE_URL = "https://virshi.app.n8n.cloud/webhook/b3d20567-46df-4c1f-8005-ff0c776f814a"

# Custom CSS
st.markdown("""
<style>
    .stApp { background-color: #F4F6F9; }
    section[data-testid="stSidebar"] { background-color: #FFFFFF; border-right: 1px solid #E0E0E0; }
    .sidebar-logo-container { display: flex; justify-content: center; margin-bottom: 10px; }
    .sidebar-logo-container img { width: 140px; }
    
    /* Cards */
    .css-1r6slb0, .css-12oz5g7, div[data-testid="stForm"] { 
        background-color: white; padding: 20px; border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05); border: 1px solid #EAEAEA;
    }
    div[data-testid="stMetric"] {
        background-color: #ffffff; border: 1px solid #e0e0e0; padding: 15px;
        border-radius: 10px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    .stButton>button { background-color: #8041F6; color: white; border-radius: 8px; border: none; font-weight: 600; }
    .stButton>button:hover { background-color: #6a35cc; }
    .upgrade-btn {
        display: block; width: 100%; background-color: #FFC107; color: #000000;
        text-align: center; padding: 8px; border-radius: 8px;
        text-decoration: none; font-weight: bold; margin-top: 10px; border: 1px solid #e0a800;
    }
    .badge-trial { background-color: #FFECB3; color: #856404; padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 0.7em; }
    .badge-active { background-color: #D4EDDA; color: #155724; padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 0.7em; }
    .sidebar-name { font-size: 14px; font-weight: 600; color: #333; margin-top: 5px;}
    .sidebar-label { font-size: 11px; color: #999; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 15px;}
</style>
""", unsafe_allow_html=True)

# --- 3. CONNECTION ---
cookie_manager = stx.CookieManager()

try:
    SUPABASE_URL = st.secrets.get("SUPABASE_URL", {}).get("url", "https://placeholder.supabase.co")
    SUPABASE_KEY = st.secrets.get("SUPABASE_URL", {}).get("key", "placeholder")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    DB_CONNECTED = False if "placeholder" in SUPABASE_URL else True
except: DB_CONNECTED = False

# Session State
if 'user' not in st.session_state: st.session_state['user'] = None
if 'user_details' not in st.session_state: st.session_state['user_details'] = {} 
if 'role' not in st.session_state: st.session_state['role'] = 'user'
if 'current_project' not in st.session_state: st.session_state['current_project'] = None
if 'gpt_history' not in st.session_state: st.session_state['gpt_history'] = []
if 'generated_prompts' not in st.session_state: st.session_state['generated_prompts'] = []

# --- 4. LOGIC & N8N INTEGRATION ---

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

def n8n_generate_prompts(brand, domain):
    """Викликає реальний вебхук n8n для генерації промптів"""
    try:
        # Реальний запит
        response = requests.post(N8N_GEN_URL, json={"brand": brand, "domain": domain}, timeout=15)
        if response.status_code == 200:
            data = response.json()
            # Очікуємо формат: { "prompts": ["q1", "q2"...] }
            return data.get('prompts', [])
        else:
            st.error(f"N8N Error: {response.status_code}")
            return []
    except Exception:
        # Fallback (якщо вебхук ще не налаштований, щоб не ламати демо)
        return [
            f"Які переваги {brand} перед конкурентами?",
            f"Відгуки користувачів про {brand} 2025",
            f"Огляд цін на послуги {brand}",
            f"Як зв'язатися з підтримкою {domain}?",
            f"Актуальні акції {brand}",
            f"Чи варто купувати у {brand}?",
            f"Порівняння {brand} з лідерами ринку",
            f"Інструкція користування {brand}",
            f"Мобільний додаток {brand}",
            f"Історія успіху {brand}"
        ]

def n8n_trigger_analysis(project_id, keywords):
    """
    Відправляє вибрані 5 запитів на n8n для глибокого аналізу.
    N8N сам запише результати в Supabase.
    """
    try:
        payload = {
            "project_id": project_id,
            "keywords": keywords,
            "user_email": st.session_state['user'].email
        }
        # Fire and forget (або чекаємо OK)
        requests.post(N8N_ANALYZE_URL, json=payload, timeout=2) 
        return True
    except:
        return False # Навіть якщо тайм-аут, n8n міг отримати запит

# --- 5. AUTH ---

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
        
        elif token == 'mock_admin':
            st.session_state['user'] = {"email": "admin@virshi.ai", "id": "m1"}
            st.session_state['role'] = "admin"
            st.session_state['user_details'] = {"first_name": "Super", "last_name": "Admin"}

def login_page():
    c1, c2, c3 = st.columns([1, 1.5, 1])
    with c2:
        st.markdown('<div style="text-align: center;"><img src="https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png" width="180"></div>', unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        
        t1, t2 = st.tabs(["🔑 Вхід", "📝 Реєстрація"])
        
        with t1:
            with st.form("login"):
                email = st.text_input("Емейл")
                password = st.text_input("Пароль", type="password")
                if st.form_submit_button("Увійти", use_container_width=True):
                    if DB_CONNECTED:
                        try:
                            res = supabase.auth.sign_in_with_password({"email": email, "password": password})
                            st.session_state['user'] = res.user
                            cookie_manager.set('virshi_auth_token', res.session.access_token)
                            r, d = get_user_role_and_details(res.user.id)
                            st.session_state['role'] = r; st.session_state['user_details'] = d
                            st.rerun()
                        except: st.error("Помилка входу")
                    else:
                        role = "admin" if "admin" in email else "user"
                        st.session_state['user'] = {"email": email}
                        st.session_state['role'] = role
                        cookie_manager.set('virshi_auth_token', 'mock_admin' if role=='admin' else 'mock_u')
                        st.rerun()

        with t2:
            with st.form("reg"):
                ne = st.text_input("Емейл"); np = st.text_input("Пароль", type="password")
                c_1, c_2 = st.columns(2)
                fn = c_1.text_input("Ім'я"); ln = c_2.text_input("Прізвище")
                if st.form_submit_button("Зареєструватися", use_container_width=True):
                    if DB_CONNECTED:
                        try:
                            res = supabase.auth.sign_up({"email": ne, "password": np, "options": {"data": {"first_name": fn}}})
                            if res.user:
                                supabase.table('profiles').insert({"id": res.user.id, "email": ne, "first_name": fn, "last_name": ln}).execute()
                                st.success("Успішно! Увійдіть.")
                        except Exception as e: st.error(f"Помилка: {e}")

def logout():
    supabase.auth.sign_out() if DB_CONNECTED else None
    cookie_manager.delete('virshi_auth_token')
    st.session_state['user'] = None
    st.session_state['current_project'] = None
    st.rerun()

# --- 6. ONBOARDING WIZARD ---

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
                    with st.spinner("AI аналізує нішу (це може зайняти до 10 сек)..."):
                        prompts = n8n_generate_prompts(brand, domain)
                        if prompts:
                            st.session_state['generated_prompts'] = prompts
                            st.session_state['onboarding_step'] = 3
                            st.rerun()
                        else:
                            st.error("Помилка генерації. Спробуйте ще раз.")
                else: st.warning("Заповніть поля")
        
        elif step == 3:
            st.subheader("Крок 2: Оберіть 5 запитів")
            st.write(f"Оберіть 5 пріоритетних запитів для **{st.session_state['temp_brand']}**:")
            
            # Якщо промпти пусті, даємо дефолтні
            opts = st.session_state['generated_prompts'] or ["Запит 1", "Запит 2"]
            selected = st.multiselect("Список запитів:", opts, default=opts[:5])
            st.caption(f"Обрано: {len(selected)} / 5")
            
            if st.button("Запустити Аналіз"):
                if len(selected) == 5:
                    with st.spinner("Створення проекту та запуск AI агентів..."):
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
                            
                            # 3. TRIGGER REAL N8N ANALYSIS
                            n8n_trigger_analysis(proj_id, selected)
                            
                            st.session_state['current_project'] = res.data[0]
                        else:
                            time.sleep(2) # Fake delay
                            st.session_state['current_project'] = {"id": "mock", "name": st.session_state['temp_brand'], "status": "trial", "created_at": "2025-01-01", "keywords": selected}
                            
                    st.success("Аналіз розпочато! Дані з'являться на дашборді через хвилину.")
                    time.sleep(2)
                    st.rerun()
                else:
                    st.error("Оберіть рівно 5 запитів")

# --- 7. DASHBOARD ---

def show_dashboard():
    proj = st.session_state.get('current_project', {})
    c1, c2 = st.columns([3, 1])
    with c1: st.title(f"Дашборд: {proj.get('name', 'My Brand')}")
    with c2: time_range = st.selectbox("Період:", ["Останні 7 днів", "Останні 30 днів"])
    st.markdown("---")
    
    # Logic to fetch real data
    sov, off, pos = 0, 0, 0
    if DB_CONNECTED and proj.get('id'):
        stats = supabase.table('dashboard_stats').select("*").eq('project_id', proj['id']).execute().data
        if stats:
            s = stats[0]
            sov, off, pos = s['sov'], s['official_source_pct'], s['avg_position']
    else:
        # Fallback if DB empty or loading
        sov, off, pos = 30.86, 50.00, 1.2

    # KPI Grid with UNIQUE KEYS to fix DuplicateElementId Error
    k1, k2, k3 = st.columns(3)
    with k1:
        with st.container(border=True):
            st.markdown(f"**ЧАСТКА ГОЛОСУ (SOV)**", help=METRIC_TOOLTIPS["sov"])
            c, ch = st.columns([1,1])
            c.markdown(f"## {sov}%")
            # FIX: Added unique key
            ch.plotly_chart(get_donut_chart(sov), use_container_width=True, key="chart_sov")
    with k2:
        with st.container(border=True):
            st.markdown(f"**% ОФІЦІЙНИХ ДЖЕРЕЛ**", help=METRIC_TOOLTIPS["official"])
            c, ch = st.columns([1,1])
            c.markdown(f"## {off}%")
            # FIX: Added unique key
            ch.plotly_chart(get_donut_chart(off), use_container_width=True, key="chart_off")
    with k3:
        with st.container(border=True):
            st.markdown(f"**ЗАГАЛЬНИЙ НАСТРІЙ**", help=METRIC_TOOLTIPS["sentiment"])
            fig = go.Figure(data=[go.Pie(labels=['Pos','Neu','Neg'], values=[60,30,10], hole=0, marker_colors=['#00C896', '#9EA0A5', '#FF4B4B'])])
            fig.update_layout(height=80, margin=dict(t=0,b=0,l=0,r=0), showlegend=False)
            # FIX: Added unique key
            st.plotly_chart(fig, use_container_width=True, key="chart_sent")
            
    k4, k5, k6 = st.columns(3)
    with k4:
        with st.container(border=True):
            st.markdown(f"**ПОЗИЦІЯ БРЕНДУ**", help=METRIC_TOOLTIPS["position"])
            st.markdown(f"<h1 style='text-align: center; color: #8041F6;'>{pos}</h1>", unsafe_allow_html=True)
            st.progress(int(100 - pos*10) if pos else 0)
    with k5:
        with st.container(border=True):
            st.markdown(f"**ПРИСУТНІСТЬ БРЕНДУ**", help=METRIC_TOOLTIPS["presence"])
            c, ch = st.columns([1,1])
            c.markdown("## 60.0%")
            # FIX: Added unique key
            ch.plotly_chart(get_donut_chart(60), use_container_width=True, key="chart_pres")
    with k6:
        with st.container(border=True):
            st.markdown(f"**ЗГАДКИ ДОМЕНУ**", help=METRIC_TOOLTIPS["domain"])
            c, ch = st.columns([1,1])
            c.markdown("## 10.0%")
            # FIX: Added unique key
            ch.plotly_chart(get_donut_chart(10), use_container_width=True, key="chart_dom")

    st.markdown("### 📈 Динаміка Позицій")
    days = 7 if "7" in time_range else 30
    df = pd.DataFrame({"Date": pd.date_range(end=datetime.today(), periods=days), "Pos": [max(1, 3+random.uniform(-1,1)) for _ in range(days)]})
    fig = px.line(df, x="Date", y="Pos", template="plotly_white")
    fig.update_yaxes(autorange="reversed")
    # FIX: Added unique key
    st.plotly_chart(fig, use_container_width=True, key="chart_line")
    
    st.markdown("### 📋 Моніторинг Запитів")
    if DB_CONNECTED and proj.get('id'):
        kws = supabase.table('keywords').select("keyword_text").eq('project_id', proj['id']).execute().data
        data = [{"Запит": k['keyword_text'], "Статус": "Аналіз..."} for k in kws]
    else:
        data = [{"Запит": k, "Статус": "Active"} for k in proj.get('keywords', [])]
    st.dataframe(pd.DataFrame(data), use_container_width=True, hide_index=True)

# --- 8. SIDEBAR ---

def sidebar_menu():
    with st.sidebar:
        st.markdown('<div class="sidebar-logo-container"><img src="https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png"></div>', unsafe_allow_html=True)
        
        if st.session_state['role'] == 'admin':
            st.markdown("### 🛠 Admin Select")
            if DB_CONNECTED:
                projs = supabase.table('projects').select("*").execute().data
                if projs:
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
        
        if st.session_state.get('current_project'):
            p = st.session_state['current_project']
            st.markdown(f"<div class='sidebar-label'>Current Brand</div>", unsafe_allow_html=True)
            badge = "<span class='badge-trial'>TRIAL</span>" if p.get('status') == 'trial' else "<span class='badge-active'>PRO</span>"
            st.markdown(f"**{p.get('brand_name') or p.get('name')}** {badge}", unsafe_allow_html=True)
            st.markdown(f"<div class='sidebar-label'>Created</div>", unsafe_allow_html=True)
            created_at = p.get('created_at', 'N/A')
            st.markdown(f"📅 {created_at[:10] if created_at else 'N/A'}")
            
            if p.get('status') == 'trial':
                st.markdown(f"""<a href="mailto:hi@virshi.ai" class="upgrade-btn">⭐ Підвищити план</a>""", unsafe_allow_html=True)
            st.divider()

        opts = ["Дашборд", "Перелік запитів", "Джерела", "Конкуренти", "Рекомендації"]
        icons = ["speedometer2", "list-ul", "hdd-network", "people", "lightbulb"]
        
        # GPT Visibility LAST
        opts.append("GPT-Visibility")
        icons.append("robot")

        if st.session_state['role'] == 'admin':
            opts.append("Адмін")
            icons.append("shield-lock")
            
        selected = option_menu(menu_title=None, options=opts, icons=icons, menu_icon="cast", default_index=0, styles={"nav-link-selected": {"background-color": "#8041F6"}, "container": {"padding": "0!important"}})
        st.divider()
        
        if st.session_state['user']:
            d = st.session_state.get('user_details', {})
            full = f"{d.get('first_name','')} {d.get('last_name','')}"
            st.markdown(f"<div class='sidebar-name'>{full}</div>", unsafe_allow_html=True)
            st.markdown("**Support:** [hi@virshi.ai](mailto:hi@virshi.ai)")
            if st.button("Вийти"): logout()
            
    return selected

# --- 9. APP ROUTER ---

def main():
    check_session()
    
    if not st.session_state['user']:
        login_page()
    elif st.session_state.get('current_project') is None and st.session_state['role'] != 'admin':
        with st.sidebar:
            if st.button("Вийти"): logout()
        onboarding_wizard()
    else:
        if st.session_state['role'] == 'admin' and not st.session_state.get('current_project'): pass
        
        page = sidebar_menu()
        
        if page == "Дашборд": show_dashboard()
        elif page == "Перелік запитів": 
            st.title("📋 Перелік запитів")
            show_dashboard() # Reuse logic for simplicity
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
