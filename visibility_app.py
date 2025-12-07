import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from supabase import create_client, Client
from streamlit_option_menu import option_menu
import extra_streamlit_components as stx
import time
from datetime import datetime, timedelta

# --- 1. CONFIGURATION ---
st.set_page_config(
    page_title="AI Visibility by Virshi",
    page_icon="👁️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 2. SUPABASE CONNECTION & COOKIES ---

# Ініціалізація менеджера куків (БЕЗ кешування, щоб уникнути помилок)
cookie_manager = stx.CookieManager()

# Підключення до Supabase
try:
    # Беремо ключі з secrets.toml
    SUPABASE_URL = st.secrets["SUPABASE_URL"]["url"]
    SUPABASE_KEY = st.secrets["SUPABASE_URL"]["key"]
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    DB_CONNECTED = True
except Exception as e:
    st.error(f"Помилка підключення до бази даних: {e}")
    DB_CONNECTED = False

# Ініціалізація Session State
if 'user' not in st.session_state: st.session_state['user'] = None
if 'user_details' not in st.session_state: st.session_state['user_details'] = {}
if 'role' not in st.session_state: st.session_state['role'] = 'user'
if 'current_project' not in st.session_state: st.session_state['current_project'] = None

# --- 3. AUTHENTICATION LOGIC (СЕСІЇ) ---

def get_user_role_and_details(user_id):
    """Отримує роль та деталі користувача з таблиці profiles"""
    try:
        data = supabase.table('profiles').select("*").eq('id', user_id).execute()
        if data.data:
            profile = data.data[0]
            return profile.get('role', 'user'), profile
    except:
        pass
    return 'user', {}

def check_session():
    """Перевіряє куки при завантаженні сторінки"""
    if st.session_state['user'] is None:
        # Чекаємо секунду, щоб кукі менеджер встиг завантажитись
        time.sleep(0.1)
        token = cookie_manager.get('virshi_auth_token')
        
        if token and DB_CONNECTED:
            try:
                # Перевіряємо токен через Supabase
                res = supabase.auth.get_user(token)
                if res.user:
                    st.session_state['user'] = res.user
                    # Підтягуємо роль і деталі з бази
                    role, details = get_user_role_and_details(res.user.id)
                    st.session_state['role'] = role
                    st.session_state['user_details'] = details
            except Exception as e:
                # Якщо токен прострочений - видаляємо
                cookie_manager.delete('virshi_auth_token')

def login_user(email, password):
    """Функція входу"""
    try:
        res = supabase.auth.sign_in_with_password({"email": email, "password": password})
        st.session_state['user'] = res.user
        
        # Зберігаємо токен у куки на 7 днів
        cookie_manager.set('virshi_auth_token', res.session.access_token, 
                         expires_at=datetime.now() + timedelta(days=7))
        
        # Отримуємо додаткові дані
        role, details = get_user_role_and_details(res.user.id)
        st.session_state['role'] = role
        st.session_state['user_details'] = details
        
        return True
    except Exception as e:
        st.error(f"Помилка входу: {e}")
        return False

def register_user(email, password, first_name, last_name):
    """Функція реєстрації"""
    try:
        # 1. Реєстрація в Auth
        res = supabase.auth.sign_up({
            "email": email, 
            "password": password,
            "options": {"data": {"first_name": first_name, "last_name": last_name}}
        })
        
        if res.user:
            # 2. Створення запису в profiles (якщо не створено автоматично тригером)
            # Примітка: краще налаштувати SQL тригер в Supabase, але можна і так:
            try:
                supabase.table('profiles').insert({
                    "id": res.user.id,
                    "email": email,
                    "first_name": first_name,
                    "last_name": last_name,
                    "role": "user"
                }).execute()
            except:
                pass # Ігноруємо, якщо тригер вже створив
            
            st.success("Реєстрація успішна! Будь ласка, увійдіть.")
            return True
    except Exception as e:
        st.error(f"Помилка реєстрації: {e}")
        return False

def logout():
    """Вихід з системи"""
    supabase.auth.sign_out()
    cookie_manager.delete('virshi_auth_token')
    st.session_state['user'] = None
    st.session_state['current_project'] = None
    st.rerun()

# --- 4. UI: LOGIN PAGE ---

def login_page():
    c1, c2, c3 = st.columns([1, 1.5, 1])
    with c2:
        st.image("https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png", width=200)
        
        tab1, tab2 = st.tabs(["Вхід", "Реєстрація"])
        
        with tab1:
            with st.form("login_form"):
                email = st.text_input("Email")
                password = st.text_input("Пароль", type="password")
                submit = st.form_submit_button("Увійти", use_container_width=True)
                
                if submit:
                    if login_user(email, password):
                        st.rerun()

        with tab2:
            with st.form("register_form"):
                new_email = st.text_input("Email")
                new_pass = st.text_input("Пароль", type="password")
                c_1, c_2 = st.columns(2)
                f_name = c_1.text_input("Ім'я")
                l_name = c_2.text_input("Прізвище")
                submit_reg = st.form_submit_button("Зареєструватися", use_container_width=True)
                
                if submit_reg:
                    if register_user(new_email, new_pass, f_name, l_name):
                        st.info("Перевірте пошту для підтвердження (якщо увімкнено) або увійдіть.")

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
