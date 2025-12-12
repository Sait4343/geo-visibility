import time
from datetime import datetime, timedelta, date
import plotly.express as px 
import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
import extra_streamlit_components as stx
from streamlit_option_menu import option_menu
from supabase import create_client, Client
from datetime import datetime, timedelta


# =========================
# 1. CONFIGURATION
# =========================

st.set_page_config(
    page_title="AI Visibility by Virshi",
    page_icon="👁️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 🔴 ПРОДАКШН N8N ВЕБХУКИ
N8N_GEN_URL = "https://virshi.app.n8n.cloud/webhook/webhook/generate-prompts"
N8N_ANALYZE_URL = "https://virshi.app.n8n.cloud/webhook/webhook/run-analysis_prod"
N8N_RECO_URL = "https://virshi.app.n8n.cloud/webhook/recommendations"  # за потреби заміниш

# Custom CSS
st.markdown(
    """
<style>
    /* 1. ЗАГАЛЬНІ НАЛАШТУВАННЯ */
    .stApp { background-color: #F4F6F9; }
    
    /* Приховування якірних посилань (ланцюжків) біля заголовків */
    [data-testid="stMarkdownContainer"] h1 > a,
    [data-testid="stMarkdownContainer"] h2 > a,
    [data-testid="stMarkdownContainer"] h3 > a,
    [data-testid="stMarkdownContainer"] h4 > a,
    [data-testid="stMarkdownContainer"] h5 > a,
    [data-testid="stMarkdownContainer"] h6 > a {
        display: none !important;
    }
    a.anchor-link { display: none !important; }

    /* 2. САЙДБАР */
    section[data-testid="stSidebar"] { 
        background-color: #FFFFFF; 
        border-right: 1px solid #E0E0E0; 
    }
    .sidebar-logo-container { display: flex; justify-content: center; margin-bottom: 10px; }
    .sidebar-logo-container img { width: 140px; }
    .sidebar-name { font-size: 14px; font-weight: 600; color: #333; margin-top: 5px;}
    .sidebar-label { font-size: 11px; color: #999; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 15px;}

    /* 3. КОНТЕЙНЕРИ І ФОРМИ */
    .css-1r6slb0, .css-12oz5g7, div[data-testid="stForm"] {
        background-color: white; padding: 20px; border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05); border: 1px solid #EAEAEA;
    }

    /* 4. МЕТРИКИ */
    div[data-testid="stMetric"] {
        background-color: #ffffff; border: 1px solid #e0e0e0; padding: 15px;
        border-radius: 10px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    .metric-card-small {
        background-color: #F0F2F6;
        border-radius: 6px;
        padding: 10px;
        text-align: center;
    }
    .metric-value {
        font-size: 18px; font-weight: bold; color: #8041F6;
    }
    .metric-label {
        font-size: 12px; color: #666;
    }

    /* 5. КНОПКИ */
    .stButton>button { 
        background-color: #8041F6; color: white; border-radius: 8px; border: none; font-weight: 600; 
        transition: background-color 0.3s;
    }
    .stButton>button:hover { background-color: #6a35cc; }
    
    .upgrade-btn {
        display: block; width: 100%; background-color: #FFC107; color: #000000;
        text-align: center; padding: 8px; border-radius: 8px;
        text-decoration: none; font-weight: bold; margin-top: 10px; border: 1px solid #e0a800;
    }

    /* 6. БЕЙДЖІ ТА СТАТУСИ */
    .badge-trial { background-color: #FFECB3; color: #856404; padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 0.7em; }
    .badge-active { background-color: #D4EDDA; color: #155724; padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 0.7em; }

    /* 7. ВІДПОВІДЬ ШІ */
    .ai-response-box {
        background-color: #ffffff;
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 20px;
        font-family: 'Source Sans Pro', sans-serif;
        line-height: 1.6;
        color: #31333F;
        box-shadow: 0 1px 2px rgba(0,0,0,0.05);
        max-height: 600px;
        overflow-y: auto;
    }
</style>
""",
    unsafe_allow_html=True,
)

# =========================
# 2. DB CONNECTION & STATE
# =========================

cookie_manager = stx.CookieManager()

try:
    SUPABASE_URL: str = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY: str = st.secrets["SUPABASE_KEY"]

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    DB_CONNECTED = True
except Exception as e:
    st.error(f"CRITICAL ERROR: Database Connection Failed. {e}")
    st.stop()

# Session State
if "user" not in st.session_state:
    st.session_state["user"] = None
if "user_details" not in st.session_state:
    st.session_state["user_details"] = {}
if "role" not in st.session_state:
    st.session_state["role"] = "user"
if "current_project" not in st.session_state:
    st.session_state["current_project"] = None
if "generated_prompts" not in st.session_state:
    st.session_state["generated_prompts"] = []
if "onboarding_step" not in st.session_state:
    st.session_state["onboarding_step"] = 2  # стартуємо одразу з кроку про бренд
if "focus_keyword_id" not in st.session_state:
    st.session_state["focus_keyword_id"] = None

# =========================
# 3. HELPERS
# =========================


def get_donut_chart(value, color="#00C896"):
    value = float(value) if value else 0.0
    remaining = max(0, 100 - value)
    fig = go.Figure(
        data=[
            go.Pie(
                values=[value, remaining],
                hole=0.75,
                marker_colors=[color, "#F0F2F6"],
                textinfo="none",
                hoverinfo="label+percent",
            )
        ]
    )
    fig.update_layout(
        showlegend=False,
        margin=dict(t=0, b=0, l=0, r=0),
        height=80,
        width=80,
        annotations=[
            dict(
                text=f"{int(value)}%",
                x=0.5,
                y=0.5,
                font_size=14,
                showarrow=False,
                font_weight="bold",
                font_color="#333",
            )
        ],
    )
    return fig


METRIC_TOOLTIPS = {
    "sov": "Частка видимості вашого бренду у відповідях ШІ порівняно з конкурентами.",
    "official": "Частка посилань на ваші офіційні ресурси.",
    "sentiment": "Тональність: Позитивна, Нейтральна або Негативна.",
    "position": "Середня позиція вашого бренду у списках рекомендацій.",
    "presence": "Відсоток запитів, де бренд був згаданий.",
    "domain": "Відсоток запитів з клікабельним посиланням на ваш домен.",
}


def n8n_generate_prompts(brand: str, domain: str, industry: str, products: str):
    """
    Викликає n8n вебхук для генерації промптів.
    Відправляємо всі 4 параметри: бренд, домен, галузь, продукти/послуги.
    """
    try:
        payload = {
            "brand": brand,
            "domain": domain,
            "industry": industry,
            "products": products,
        }
        response = requests.post(N8N_GEN_URL, json=payload, timeout=20)

        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                return data
            return data.get("prompts", [])
        else:
            st.error(f"N8N Error: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        st.error(f"Помилка з'єднання з N8N: {e}")
        return []


def n8n_trigger_analysis(project_id, keywords, brand_name, models=None):
    """
    Відправляє запит на n8n для аналізу.
    FIX: Виправлено помилку NoneType при перевірці статусу.
    """
    
    # 1. Мапінг назв (UI -> Technical)
    MODEL_MAPPING = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }

    # 2. 🔒 ПЕРЕВІРКА СТАТУСУ (БЛОКУВАННЯ)
    current_proj = st.session_state.get("current_project")
    
    # 🔥 FIX: Якщо проекту немає (None), вважаємо статус 'trial' (для онбордингу), 
    # або перевіряємо, чи це не перший запуск.
    if current_proj is None:
        status = "trial" 
    else:
        status = current_proj.get("status", "trial")
    
    # Якщо статус заблокований або термін дії вийшов - зупиняємо
    if status in ["blocked", "expired"]:
        st.error(f"⛔ Дія недоступна. Ваш статус: {status.upper()}. Будь ласка, зв'яжіться з адміністратором.")
        return False

    try:
        # Отримуємо email безпечно
        user = st.session_state.get("user")
        user_email = user.email if user else "no-reply@virshi.ai"
        
        if isinstance(keywords, str):
            keywords = [keywords]

        # Якщо моделі не обрані або пусті, беремо дефолтну
        if not models:
            models = ["Perplexity"]

        success_count = 0

        # 3. ОТРИМУЄМО ОФІЦІЙНІ ДЖЕРЕЛА (WHITELIST)
        try:
            assets_resp = supabase.table("official_assets")\
                .select("domain_or_url")\
                .eq("project_id", project_id)\
                .execute()
            official_assets = [item["domain_or_url"] for item in assets_resp.data] if assets_resp.data else []
        except Exception as e:
            print(f"Error fetching assets: {e}")
            official_assets = []

        # 4. ЦИКЛ ВІДПРАВКИ
        for ui_model_name in models:
            tech_model_id = MODEL_MAPPING.get(ui_model_name, ui_model_name)

            payload = {
                "project_id": project_id,
                "keywords": keywords, 
                "brand_name": brand_name,
                "user_email": user_email,
                "provider": tech_model_id,
                "models": [tech_model_id],
                "official_assets": official_assets
            }
            
            try:
                response = requests.post(N8N_ANALYZE_URL, json=payload, timeout=10)
                
                if response.status_code == 200:
                    success_count += 1
                else:
                    st.error(f"Помилка n8n ({ui_model_name}): {response.text}")
                    
            except Exception as inner_e:
                st.error(f"Не вдалося запустити {ui_model_name}: {inner_e}")

        return success_count > 0
            
    except Exception as e:
        st.error(f"Критична помилка запуску: {e}")
        return False

def n8n_request_recommendations(project, topic: str, brief: str):
    """
    Надсилає запит на n8n для генерації рекомендацій.
    topic: 'pr' | 'digital' | 'creative'
    """
    try:
        payload = {
            "project_id": project["id"],
            "brand_name": project.get("brand_name"),
            "domain": project.get("domain"),
            "topic": topic,
            "brief": brief,
            "user_email": st.session_state["user"].email
            if st.session_state.get("user")
            else None,
        }
        resp = requests.post(N8N_RECO_URL, json=payload, timeout=40)
        if resp.status_code != 200:
            st.error(f"N8N recommendation error: {resp.status_code} - {resp.text}")
            return []

        data = resp.json()
        if isinstance(data, list):
            return data
        return data.get("recommendations", [])
    except Exception as e:
        st.error(f"Помилка запиту рекомендацій: {e}")
        return []


# =========================
# 4. AUTH & USER LOGIC
# =========================


def load_user_project(user_id: str) -> bool:
    try:
        res = supabase.table("projects").select("*").eq("user_id", user_id).execute()
        if res.data and len(res.data) > 0:
            st.session_state["current_project"] = res.data[0]
            return True
    except Exception:
        pass
    return False


def get_user_role_and_details(user_id: str):
    try:
        data = supabase.table("profiles").select("*").eq("id", user_id).execute()
        if data.data:
            p = data.data[0]
            return p.get("role", "user"), {
                "first_name": p.get("first_name"),
                "last_name": p.get("last_name"),
            }
    except Exception:
        pass
    return "user", {}


def check_session():
    if st.session_state["user"] is None:
        time.sleep(0.1)
        token = cookie_manager.get("virshi_auth_token")

        if token:
            try:
                res = supabase.auth.get_user(token)
                if getattr(res, "user", None):
                    st.session_state["user"] = res.user
                    role, details = get_user_role_and_details(res.user.id)
                    st.session_state["role"] = role
                    st.session_state["user_details"] = details
                    load_user_project(res.user.id)
                else:
                    cookie_manager.delete("virshi_auth_token")
            except Exception:
                cookie_manager.delete("virshi_auth_token")


def login_user(email: str, password: str):
    try:
        res = supabase.auth.sign_in_with_password(
            {"email": email, "password": password}
        )
        if not res.user:
            st.error("Не вдалося увійти. Перевірте email та пароль.")
            return

        st.session_state["user"] = res.user
        cookie_manager.set(
            "virshi_auth_token",
            res.session.access_token,
            expires_at=datetime.now() + timedelta(days=7),
        )

        role, details = get_user_role_and_details(res.user.id)
        st.session_state["role"] = role
        st.session_state["user_details"] = details

        if load_user_project(res.user.id):
            st.success("Вхід успішний!")

        st.rerun()
    except Exception:
        st.error(
            "Помилка входу: невірний логін, пароль або налаштування підтвердження email."
        )


def register_user(email: str, password: str, first: str, last: str) -> bool:
    """
    Реєстрація нового користувача + запис first_name / last_name в таблицю profiles.
    """
    try:
        res = supabase.auth.sign_up(
            {
                "email": email,
                "password": password,
                "options": {"data": {"first_name": first, "last_name": last}},
            }
        )

        if res.user:
            # явне створення профілю
            try:
                supabase.table("profiles").insert(
                    {
                        "id": res.user.id,
                        "email": email,
                        "first_name": first,
                        "last_name": last,
                        "role": "user",
                    }
                ).execute()
            except Exception:
                pass

            if res.session:
                st.success("Реєстрація успішна! Виконуємо вхід...")
                st.session_state["user"] = res.user
                cookie_manager.set(
                    "virshi_auth_token",
                    res.session.access_token,
                    expires_at=datetime.now() + timedelta(days=7),
                )
                role, details = get_user_role_and_details(res.user.id)
                st.session_state["role"] = role
                st.session_state["user_details"] = details
                load_user_project(res.user.id)
                st.rerun()
            else:
                st.success(
                    "Реєстрація успішна! Перевірте пошту, підтвердіть email "
                    "та увійдіть на вкладці «Вхід»."
                )
            return True

        st.error("Не вдалося створити користувача. Перевірте налаштування Auth.")
    except Exception as e:
        if "already registered" in str(e):
            st.warning("Користувач вже існує. Спробуйте увійти.")
        else:
            st.error(f"Помилка реєстрації: {e}")
    return False


def logout():
    """
    Надійний вихід із системи.
    """
    # 1. Видаляємо куку (Token)
    try:
        cookie_manager.delete("virshi_auth_token")
    except Exception:
        pass

    # 2. Виходимо з Supabase (на стороні сервера)
    try:
        supabase.auth.sign_out()
    except Exception:
        pass

    # 3. 🔥 ПОВНЕ очищення Session State
    # Це видаляє всі змінні: user, current_project, налаштування фільтрів тощо.
    st.session_state.clear()

    # 4. Ініціалізуємо критичні змінні, щоб не було помилок до перезавантаження
    st.session_state["user"] = None
    
    # 5. Пауза, щоб браузер встиг фізично видалити куку
    time.sleep(1)

    # 6. Перезавантаження сторінки
    st.rerun()


def login_page():
    c_l, c_center, c_r = st.columns([1, 1.5, 1])
    with c_center:
        st.markdown(
            '<div style="text-align: center;"><img src="https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png" width="180"></div>',
            unsafe_allow_html=True,
        )
        st.markdown("<br>", unsafe_allow_html=True)

        t1, t2 = st.tabs(["🔑 Вхід", "📝 Реєстрація"])

        # ВХІД
        with t1:
            with st.form("login"):
                email = st.text_input("Емейл")
                password = st.text_input("Пароль", type="password")
                if st.form_submit_button("Увійти", use_container_width=True):
                    if email and password:
                        login_user(email, password)
                    else:
                        st.warning("Введіть емейл та пароль.")

        # РЕЄСТРАЦІЯ
        with t2:
            with st.form("reg"):
                ne = st.text_input("Емейл")
                np = st.text_input("Пароль", type="password")
                c1, c2 = st.columns(2)
                fn = c1.text_input("Ім'я")
                ln = c2.text_input("Прізвище")
                if st.form_submit_button("Зареєструватися", use_container_width=True):
                    if ne and np and fn:
                        register_user(ne, np, fn, ln)
                    else:
                        st.warning("Всі поля обов'язкові.")


# =========================
# 5. ONBOARDING
# =========================


def onboarding_wizard():
    """
    Майстер створення першого проекту (2 етапи).
    Оновлено: Стиль карток, редагування, виправлення помилки втрати сесії.
    """
    import requests
    import time
    
    # 🚨 Ініціалізація стейту
    if "onboarding_stage" not in st.session_state:
        st.session_state["onboarding_stage"] = 2
        st.session_state["generated_prompts"] = []
    
    # CSS для стилізації карток та вирівнювання
    st.markdown("""
    <style>
        div[data-testid="stVerticalBlock"] > div[data-testid="stHorizontalBlock"] {
            align-items: center;
        }
        .prompt-card {
            background-color: white;
            padding: 15px;
            border-radius: 8px;
            border: 1px solid #E0E0E0;
            margin-bottom: 10px;
        }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown("## 🚀 Налаштування Проекту")

    # Використовуємо .get для безпеки
    step = st.session_state.get("onboarding_step", 2) 

    with st.container(border=True):

        # ========================================================
        # STEP 2 – дані про бренд (ВВІД)
        # ========================================================
        if step == 2:
            st.subheader("Крок 1: Введіть дані про ваш бренд")

            c1, c2 = st.columns(2)
            with c1:
                brand = st.text_input("Назва бренду", placeholder="Monobank", value=st.session_state.get("temp_brand", ""))
                industry = st.text_input("Галузь бренду / ніша", placeholder="Фінтех, Банкінг", value=st.session_state.get("temp_industry", ""))
                
            with c2:
                domain = st.text_input("Домен (офіційний сайт)", placeholder="monobank.ua", value=st.session_state.get("temp_domain", ""))
                st.markdown("<p style='color: #6c5ce7; margin-top: 10px;'>📍 **Регіон:** UA (Фіксовано)</p>", unsafe_allow_html=True)
            
            products = st.text_area(
                "Продукти / Послуги (перелічіть через кому або у стовпчик)", 
                help="На основі цього буде сформовано запити.",
                value=st.session_state.get("temp_products", "")
            )

            if st.button("Згенерувати запити"):
                if brand and domain and industry and products:
                    # 1. Зберігаємо дані у тимчасовий стейт
                    st.session_state["temp_brand"] = brand
                    st.session_state["temp_domain"] = domain
                    st.session_state["temp_industry"] = industry
                    st.session_state["temp_products"] = products
                    st.session_state["temp_region"] = "UA"

                    with st.spinner("Генеруємо релевантні запити через n8n AI Agent..."):
                        prompts = n8n_generate_prompts(brand, domain, industry, products)
                        
                        if prompts and len(prompts) > 0:
                            st.session_state["generated_prompts"] = prompts
                            st.session_state["onboarding_step"] = 3
                            st.rerun()
                        else:
                            st.error("AI не повернув результатів. Спробуйте змінити опис продуктів.")
                else:
                    st.warning("Будь ласка, заповніть всі 4 поля.")

        # ========================================================
        # STEP 3 – Редагування та Вибір (КОНФІРМАЦІЯ)
        # ========================================================
        elif step == 3:
            # 🛡️ SAFETY CHECK: Перевіряємо, чи не зникли дані сесії
            if not st.session_state.get("temp_brand") or not st.session_state.get("temp_domain"):
                st.warning("⚠️ Дані сесії застаріли. Будь ласка, поверніться на крок назад.")
                if st.button("⬅ Назад до вводу даних"):
                    st.session_state["onboarding_step"] = 2
                    st.rerun()
                return

            st.subheader("Крок 2: Перевірка та редагування запитів")
            st.info("Ви можете відредагувати текст кожного запиту перед запуском. Оберіть галочками ті, що підуть в роботу.")

            prompts_list = st.session_state.get("generated_prompts", [])
            
            if not prompts_list:
                st.error("Помилка: Список запитів порожній.")
                if st.button("Повернутися назад"): 
                    st.session_state["onboarding_step"] = 2
                    st.rerun()
                return

            st.markdown("<br>", unsafe_allow_html=True)
            
            # Словник для збереження обраних (індекс -> текст)
            selected_indices = []

            # --- ЦИКЛ ВИВОДУ КАРТОК (Card Style) ---
            for i, kw in enumerate(prompts_list):
                # Ключ для відстеження режиму редагування
                edit_key = f"edit_mode_row_{i}"
                
                # 🔥 STYLING: Використовуємо container(border=True) для створення ефекту картки
                with st.container(border=True):
                    # Сітка: Чекбокс | Текст | Кнопка
                    c_check, c_text, c_btn = st.columns([0.5, 9, 1])
                    
                    # 1. Чекбокс
                    with c_check:
                        is_checked = st.checkbox("", value=True, key=f"chk_final_{i}", label_visibility="collapsed")
                        if is_checked:
                            selected_indices.append(i)

                    # Перевіряємо режим редагування
                    if st.session_state.get(edit_key, False):
                        # --- РЕЖИМ РЕДАГУВАННЯ ---
                        with c_text:
                            new_val = st.text_input("Редагування", value=kw, key=f"input_kw_{i}", label_visibility="collapsed")
                        
                        with c_btn:
                            # Кнопка Зберегти (Зелена галочка або дискета)
                            if st.button("💾", key=f"save_kw_{i}", help="Зберегти зміни"):
                                st.session_state["generated_prompts"][i] = new_val
                                st.session_state[edit_key] = False
                                st.rerun()
                    else:
                        # --- РЕЖИМ ПЕРЕГЛЯДУ ---
                        with c_text:
                            # Виводимо текст жирним, трохи більшим шрифтом
                            st.markdown(f"<div style='font-size:16px; padding-top:5px;'>{kw}</div>", unsafe_allow_html=True)
                        
                        with c_btn:
                            # Кнопка Редагувати (Олівець)
                            if st.button("✏️", key=f"edit_kw_{i}", help="Редагувати текст"):
                                st.session_state[edit_key] = True
                                st.rerun()

            st.markdown("---")
            
            # Збираємо фінальний список
            final_kws_to_send = [st.session_state["generated_prompts"][idx] for idx in selected_indices]

            c_info, c_action = st.columns([2, 1])
            with c_info:
                st.markdown(f"**Обрано до аналізу:** {len(final_kws_to_send)} запитів")
                st.caption("Натисніть кнопку справа, щоб створити проект і почати.")

            with c_action:
                # --- КНОПКА ЗАПУСКУ ---
                if st.button("🚀 Зберегти та Запустити аналіз", type="primary", use_container_width=True):
                    if len(final_kws_to_send) > 0:
                        with st.spinner("Створення проекту та запуск Gemini..."):
                            try:
                                user_id = st.session_state["user"].id
                                
                                # Беремо дані з сесії (вони вже перевірені на початку кроку)
                                brand_name = st.session_state.get("temp_brand")
                                domain_name = st.session_state.get("temp_domain")
                                region_name = "UA"
                                
                                # 1. Створення проекту в БД
                                res = supabase.table("projects").insert({
                                    "user_id": user_id,
                                    "brand_name": brand_name,
                                    "domain": domain_name,
                                    "region": region_name, 
                                    "status": "trial",
                                }).execute()

                                if not res.data:
                                    raise Exception("Не вдалося створити проект в базі.")

                                proj_data = res.data[0]
                                proj_id = proj_data["id"]

                                # 2. Записуємо ВІДРЕДАГОВАНІ ключові слова
                                kws_data = [
                                    {
                                        "project_id": proj_id, 
                                        "keyword_text": kw_text, 
                                        "is_active": True, 
                                        "is_cron_active": False
                                    } for kw_text in final_kws_to_send
                                ]
                                supabase.table("keywords").insert(kws_data).execute()
                                
                                # 3. Записуємо офіційний домен
                                clean_domain = domain_name.replace("https://", "").replace("http://", "").strip().rstrip("/")
                                supabase.table("official_assets").insert(
                                    {"project_id": proj_id, "domain_or_url": clean_domain, "type": "website"}
                                ).execute()

                                # 4. ВІДПРАВЛЯЄМО НА N8N (Gemini Only)
                                n8n_trigger_analysis(
                                    proj_id, 
                                    final_kws_to_send, 
                                    brand_name,
                                    models=["Google Gemini"] 
                                )

                                # 5. Фінал
                                st.session_state["current_project"] = proj_data
                                st.session_state["onboarding_step"] = 2 
                                st.success("Успіх! Проект створено, аналіз запущено.")
                                time.sleep(2)
                                st.rerun()
                                
                            except Exception as e:
                                st.error(f"Помилка при створенні: {e}")
                    else:
                        st.warning("Оберіть хоча б один запит.")
                    
# =========================
# 6. DASHBOARD
# =========================

def show_competitors_page():
    """
    Сторінка глибокого конкурентного аналізу.
    Оновлено: 
    - Вкладка 'Частота згадки': st.area_chart + таблиця зліва в стилі загального рейтингу.
    """
    import pandas as pd
    import plotly.express as px
    import streamlit as st
    
    # --- 0. ПІДКЛЮЧЕННЯ ---
    if 'supabase' not in globals():
        if 'supabase' in st.session_state:
            supabase = st.session_state['supabase']
        else:
            st.error("🚨 Помилка: Змінна 'supabase' не знайдена.")
            return
    else:
        supabase = globals()['supabase']

    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект.")
        return

    MODEL_MAPPING = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }

    st.title("👥 Аналіз Конкурентів")

    # --- 1. ЗАВАНТАЖЕННЯ ДАНИХ ---
    try:
        # A. Сканування
        scans_resp = supabase.table("scan_results")\
            .select("id, provider, keyword_id, created_at")\
            .eq("project_id", proj["id"])\
            .execute()
        
        if not scans_resp.data:
            st.info("Даних немає. Запустіть сканування.")
            return
            
        df_scans = pd.DataFrame(scans_resp.data)
        
        # B. Ключові слова
        kw_resp = supabase.table("keywords").select("id, keyword_text").eq("project_id", proj["id"]).execute()
        kw_map = {k['id']: k['keyword_text'] for k in kw_resp.data}
        df_scans['keyword_text'] = df_scans['keyword_id'].map(kw_map)

        # C. Згадки брендів
        scan_ids = df_scans['id'].tolist()
        mentions_resp = supabase.table("brand_mentions")\
            .select("*")\
            .in_("scan_result_id", scan_ids)\
            .execute()
        
        if not mentions_resp.data:
            st.info("Брендів не знайдено.")
            return

        df_mentions = pd.DataFrame(mentions_resp.data)

        # D. Master Data
        df_full = pd.merge(df_mentions, df_scans, left_on='scan_result_id', right_on='id', how='left')

    except Exception as e:
        st.error(f"Помилка обробки даних: {e}")
        return

    # --- 2. ФІЛЬТРИ ---
    with st.container(border=True):
        c1, c2 = st.columns(2)
        with c1:
            all_models = list(MODEL_MAPPING.keys())
            sel_models = st.multiselect("🤖 Фільтр по LLM:", all_models, default=all_models)
            sel_tech_models = [MODEL_MAPPING[m] for m in sel_models]

        with c2:
            all_kws = df_full['keyword_text'].dropna().unique().tolist()
            sel_kws = st.multiselect("🔎 Фільтр по Запитах:", all_kws, default=all_kws)

    # Застосування фільтрів
    if sel_tech_models:
        mask_model = df_full['provider'].apply(lambda x: any(t in str(x) for t in sel_tech_models))
    else:
        mask_model = df_full['provider'].apply(lambda x: False)

    if sel_kws:
        mask_kw = df_full['keyword_text'].isin(sel_kws)
    else:
        mask_kw = df_full['keyword_text'].apply(lambda x: False)

    df_filtered = df_full[mask_model & mask_kw]

    if df_filtered.empty:
        st.warning("За обраними фільтрами даних немає.")
        return

    # --- 3. АГРЕГАЦІЯ ---
    
    # Хелпер: Текст -> Число
    def sentiment_to_score(s):
        if s == 'Позитивний': return 100
        if s == 'Негативний': return 0
        return 50 # Нейтральний
    
    df_filtered['sent_score_num'] = df_filtered['sentiment_score'].apply(sentiment_to_score)

    stats = df_filtered.groupby('brand_name').agg(
        Mentions=('id_x', 'count'),
        Avg_Rank=('rank_position', 'mean'),
        Avg_Sentiment_Num=('sent_score_num', 'mean'),
        Is_My_Brand=('is_my_brand', 'max')
    ).reset_index()

    # Хелпер: Число -> Текст
    def get_sentiment_text(score):
        if score >= 60: return "Позитивна"
        if score <= 40: return "Негативна"
        return "Нейтральна"

    stats['Reputation_Text'] = stats['Avg_Sentiment_Num'].apply(get_sentiment_text)
    stats['Show'] = True 

    # --- 4. ВІДОБРАЖЕННЯ (ВКЛАДКИ) ---
    st.write("") 
    
    tab_list, tab_freq, tab_sent, tab_rank = st.tabs([
        "📋 Детальний рейтинг", 
        "📊 Частота згадки", 
        "⭐ Тональність", 
        "🏆 Середня позиція"
    ])

    # === TAB 1: ДЕТАЛЬНИЙ РЕЙТИНГ ===
    with tab_list:
        st.markdown("##### 📋 Зведена таблиця")
        
        display_df = stats.copy().sort_values('Mentions', ascending=False)
        display_df['Сер. Позиція'] = display_df['Avg_Rank'].apply(lambda x: f"#{x:.1f}")
        display_df['Is_My_Brand'] = display_df['Is_My_Brand'].apply(lambda x: True if x else False)

        st.dataframe(
            display_df[['brand_name', 'Mentions', 'Reputation_Text', 'Сер. Позиція', 'Is_My_Brand']],
            use_container_width=True,
            column_config={
                "brand_name": "Бренд",
                "Mentions": st.column_config.ProgressColumn("Згадок", format="%d", min_value=0, max_value=int(stats['Mentions'].max())),
                "Reputation_Text": st.column_config.TextColumn("Репутація"),
                "Is_My_Brand": st.column_config.CheckboxColumn("Цільовий бренд", disabled=True)
            },
            hide_index=True
        )

    # === TAB 2: ЧАСТОТА ЗГАДКИ (AREA CHART) ===
    with tab_freq:
        st.markdown("##### 📊 Частота згадки (Area Chart)")
        
        col_table, col_chart = st.columns([1.8, 2.2])

        with col_table:
            # Таблиця налаштувань (ідентична по стилю до Tab 1)
            df_freq_editor = stats[['Show', 'brand_name', 'Mentions', 'Is_My_Brand']].copy()
            df_freq_editor = df_freq_editor.sort_values('Mentions', ascending=False)

            edited_freq_df = st.data_editor(
                df_freq_editor,
                column_config={
                    "Show": st.column_config.CheckboxColumn("Show", width="small"),
                    "brand_name": st.column_config.TextColumn("Бренд", disabled=True),
                    "Mentions": st.column_config.ProgressColumn(
                        "Згадок", 
                        format="%d", 
                        min_value=0, 
                        max_value=int(stats['Mentions'].max())
                    ),
                    "Is_My_Brand": st.column_config.CheckboxColumn("Цільовий", disabled=True, width="small")
                },
                hide_index=True,
                use_container_width=True,
                key="editor_freq"
            )

        with col_chart:
            # Дані для графіка
            chart_data = edited_freq_df[edited_freq_df['Show'] == True]
            
            if not chart_data.empty:
                # Готуємо дані для Area Chart (Індекс - Бренд, Значення - Згадки)
                # st.area_chart використовує індекс як вісь X
                chart_view = chart_data.set_index('brand_name')[['Mentions']]
                
                st.markdown("**Динаміка згадок:**")
                st.area_chart(chart_view, color="#00C896")
            else:
                st.info("Оберіть хоча б один бренд у таблиці зліва.")

    # === TAB 3: ТОНАЛЬНІСТЬ (STACKED BAR) ===
    with tab_sent:
        st.markdown("##### ⭐ Аналіз Тональності")
        st.caption("Співвідношення: Позитивні vs Нейтральні vs Негативні.")

        # Агрегація для Stacked Bar
        sent_distribution = df_filtered.groupby(['brand_name', 'sentiment_score']).size().reset_index(name='count')
        total_per_brand = sent_distribution.groupby('brand_name')['count'].transform('sum')
        sent_distribution['percentage'] = (sent_distribution['count'] / total_per_brand * 100).round(1)

        # Керування
        col_list, col_chart = st.columns([1.5, 2.5])
        
        with col_list:
            df_sent_editor = stats[['Show', 'brand_name', 'Reputation_Text']].sort_values('brand_name')
            edited_sent_df = st.data_editor(
                df_sent_editor,
                column_config={
                    "Show": st.column_config.CheckboxColumn("Show", width="small"),
                    "brand_name": "Бренд",
                    "Reputation_Text": "Репутація"
                },
                hide_index=True,
                use_container_width=True,
                key="editor_sent"
            )

        with col_chart:
            selected_brands = edited_sent_df[edited_sent_df['Show'] == True]['brand_name'].tolist()
            chart_data_sent = sent_distribution[sent_distribution['brand_name'].isin(selected_brands)]

            if not chart_data_sent.empty:
                color_map_sent = {
                    "Позитивний": "#00C896",   
                    "Нейтральний": "#E0E0E0",  
                    "Негативний": "#FF4B4B"    
                }
                
                fig_stack = px.bar(
                    chart_data_sent,
                    y="brand_name",
                    x="percentage",
                    color="sentiment_score",
                    orientation='h',
                    text="percentage",
                    color_discrete_map=color_map_sent,
                    category_orders={"sentiment_score": ["Негативний", "Нейтральний", "Позитивний"]},
                    height=500
                )
                
                fig_stack.update_traces(texttemplate='%{text}%', textposition='inside')
                fig_stack.update_layout(
                    barmode='stack',
                    xaxis_title="Частка (%)",
                    yaxis_title="",
                    legend_title="Тональність",
                    plot_bgcolor='rgba(0,0,0,0)',
                    margin=dict(l=0, r=0, t=30, b=0)
                )
                st.plotly_chart(fig_stack, use_container_width=True)
            else:
                st.info("Оберіть бренд для аналізу.")

    # === TAB 4: СЕРЕДНЯ ПОЗИЦІЯ (DONUT INVERSE) ===
    with tab_rank:
        st.markdown("##### 🏆 Середня позиція (Чим менше число - тим краще)")
        
        col_rank_table, col_rank_chart = st.columns([1.5, 2])

        with col_rank_table:
            df_rank_editor = stats[['Show', 'brand_name', 'Avg_Rank', 'Is_My_Brand']].sort_values('Avg_Rank', ascending=True)

            edited_rank_df = st.data_editor(
                df_rank_editor,
                column_config={
                    "Show": st.column_config.CheckboxColumn("Show", width="small"),
                    "brand_name": st.column_config.TextColumn("Бренд", disabled=True),
                    "Avg_Rank": st.column_config.NumberColumn("Ранг", format="%.1f"),
                    "Is_My_Brand": None
                },
                hide_index=True,
                use_container_width=True,
                key="editor_rank"
            )

        with col_rank_chart:
            chart_data_rank = edited_rank_df[edited_rank_df['Show'] == True].copy()
            if not chart_data_rank.empty:
                # Логіка інверсії (для візуального розміру)
                max_rank_val = chart_data_rank['Avg_Rank'].max()
                base_val = max_rank_val + 2 
                chart_data_rank['Inverse_Score'] = base_val - chart_data_rank['Avg_Rank']

                fig_rank = px.pie(
                    chart_data_rank,
                    names='brand_name',
                    values='Inverse_Score', # Розмір сектора
                    hole=0.6,
                    color='Is_My_Brand',
                    color_discrete_map={True: '#00C896', False: '#FFCE56', 1: '#00C896', 0: '#FFCE56'},
                    hover_data=['brand_name']
                )
                # У підписах показуємо РЕАЛЬНИЙ ранг!
                fig_rank.update_traces(
                    customdata=chart_data_rank[['Avg_Rank']],
                    textinfo='label',
                    hovertemplate = "<b>%{label}</b><br>Середнє місце: %{customdata[0]:.1f}"
                )
                
                leader = chart_data_rank.iloc[0]
                fig_rank.update_layout(
                    showlegend=False, 
                    margin=dict(t=20, b=20, l=20, r=20), 
                    height=350,
                    annotations=[dict(text=f"Лідер:<br>{leader['brand_name']}<br>#{leader['Avg_Rank']:.1f}", x=0.5, y=0.5, font_size=14, showarrow=False)]
                )
                st.plotly_chart(fig_rank, use_container_width=True)
            else:
                st.info("Оберіть бренд.")

def show_dashboard():
    import plotly.graph_objects as go
    from datetime import datetime, timedelta, time as dt_time
    
    # Перевірка підключення (Safe Check)
    if 'supabase' not in globals():
        if 'supabase' in st.session_state:
            supabase = st.session_state['supabase']
        else:
            st.error("🚨 Помилка: Змінна 'supabase' не знайдена.")
            return
    else:
        supabase = globals()['supabase']

    proj = st.session_state.get("current_project", {})
    if not proj:
        st.info("Спочатку створіть проект.")
        return

    # --- 1. ВИЗНАЧЕННЯ ПЕРІОДУ ---
    today = datetime.now().date()
    
    try:
        first_scan = supabase.table("scan_results")\
            .select("created_at")\
            .eq("project_id", proj["id"])\
            .order("created_at", desc=False)\
            .limit(1)\
            .execute()
        
        if first_scan.data:
            min_date_str = first_scan.data[0]['created_at']
            min_date = datetime.fromisoformat(min_date_str.replace('Z', '+00:00')).date()
        else:
            min_date = today - timedelta(days=1)
    except Exception:
        min_date = today - timedelta(days=30)

    # ВЕРХНЯ ПАНЕЛЬ
    c_title, c_date = st.columns([3, 1])
    with c_title:
        st.title(f"📊 Дашборд: {proj.get('brand_name')}")
    
    with c_date:
        date_range = st.date_input(
            "Період аналізу:",
            value=(min_date, today),
            min_value=min_date,
            max_value=today,
            format="DD.MM.YYYY"
        )

    st.markdown("---")

    if isinstance(date_range, tuple):
        if len(date_range) == 2:
            start_date, end_date = date_range
        elif len(date_range) == 1:
            start_date = date_range[0]
            end_date = today
        else:
            start_date = min_date
            end_date = today
    else:
        start_date = min_date
        end_date = today

# --- 2. ЗАВАНТАЖЕННЯ ДАНИХ ---
    try:
        # Перетворюємо дати в ISO формат
        iso_start = datetime.combine(start_date, dt_time.min).isoformat()
        iso_end = datetime.combine(end_date, dt_time.max).isoformat()

        # A. ID Сканувань
        scans_query = supabase.table("scan_results")\
            .select("id, created_at, keyword_id")\
            .eq("project_id", proj["id"])\
            .gte("created_at", iso_start)\
            .lte("created_at", iso_end)\
            .execute()
        
        scan_ids = [s['id'] for s in scans_query.data]
        
        # --- ПЕРЕВІРКА НА ПУСТОТУ ---
        if not scan_ids:
            # Тут ми виходимо, якщо немає сканувань взагалі
            st.warning(f"🔍 За період з {start_date.strftime('%d.%m')} по {end_date.strftime('%d.%m')} даних не знайдено.")
            st.info("👉 Запустіть нове сканування.")
            return

        # B. Згадки та Джерела
        mentions_resp = supabase.table("brand_mentions").select("*").in_("scan_result_id", scan_ids).execute()
        sources_resp = supabase.table("extracted_sources").select("*").in_("scan_result_id", scan_ids).execute()
        keywords_resp = supabase.table("keywords").select("id, keyword_text").eq("project_id", proj["id"]).execute()
        
        df_mentions = pd.DataFrame(mentions_resp.data)
        df_sources = pd.DataFrame(sources_resp.data)
        kw_map = {k['id']: k['keyword_text'] for k in keywords_resp.data}
        
        # 🔥 FIX: ЗАХИСТ ВІД KEYERROR - Гарантуємо наявність колонок, навіть якщо відповідь Supabase порожня
        
        # Якщо дані прийшли пустими, створюємо DF з необхідними колонками
        if df_mentions.empty:
            df_mentions = pd.DataFrame(columns=['mention_count', 'is_my_brand', 'sentiment_score', 'rank_position', 'scan_result_id'])
        
        if df_sources.empty:
            df_sources = pd.DataFrame(columns=['is_official', 'domain', 'scan_result_id'])


    except Exception as e:
        st.error(f"Помилка обробки даних: {e}")
        return

    # --- 3. РОЗРАХУНОК МЕТРИК ---
    
    # 1. SOV
    total_mentions = df_mentions['mention_count'].sum() if not df_mentions.empty else 0
    my_mentions = df_mentions[df_mentions['is_my_brand'] == True]['mention_count'].sum() if not df_mentions.empty else 0
    sov = (my_mentions / total_mentions * 100) if total_mentions > 0 else 0.0

    # 2. Офіційні джерела
    total_sources = len(df_sources)
    official_sources = len(df_sources[df_sources['is_official'] == True])
    official_pct = (official_sources / total_sources * 100) if total_sources > 0 else 0.0

    # 3. Sentiment Data
    if not df_mentions.empty:
        my_brand_rows = df_mentions[df_mentions['is_my_brand'] == True].copy()
    else:
        my_brand_rows = pd.DataFrame()

    # 4. Позиція
    if not my_brand_rows.empty:
        found_rows = my_brand_rows[my_brand_rows['rank_position'].notnull()]
        avg_pos = found_rows['rank_position'].mean() if not found_rows.empty else 0
    else:
        avg_pos = 0

    # 5. Присутність
    total_scans_count = len(scan_ids)
    scans_with_me = 0
    if not my_brand_rows.empty:
        scans_with_me = my_brand_rows['scan_result_id'].nunique()
    visibility_rate = (scans_with_me / total_scans_count * 100) if total_scans_count > 0 else 0.0

    # --- 4. ВІЗУАЛІЗАЦІЯ ---
    
    def make_donut(value, color="#00C896"):
        fig = go.Figure(data=[go.Pie(
            values=[value, 100-value], hole=.75,
            marker_colors=[color, "#EEF0F2"], textinfo='none', hoverinfo='none'
        )])
        fig.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), height=120,
            annotations=[dict(text=f"{value:.1f}%", x=0.5, y=0.5, font_size=20, showarrow=False, font_weight='bold')])
        return fig

    st.markdown("""
    <style>
        .dash-card { background-color: white; border: 1px solid #E0E0E0; border-radius: 10px; padding: 20px; text-align: center; height: 280px; display: flex; flex-direction: column; justify-content: space-between; }
        .dash-title { font-size: 12px; text-transform: uppercase; color: #888; font-weight: 600; margin-bottom: 10px; }
    </style>
    """, unsafe_allow_html=True)

    # === РЯДОК 1 (СТВОРЮЄМО КОЛОНКИ ТУТ) ===
    # ⚠️ Ось цей рядок був пропущений, тому виникала помилка
    r1_c1, r1_c2, r1_c3 = st.columns(3)

    # Картка 1: SOV
    with r1_c1:
        with st.container(border=True):
            st.markdown("<div class='dash-title'>ЧАСТКА ГОЛОСУ (SOV)</div>", unsafe_allow_html=True)
            st.plotly_chart(make_donut(sov, "#00C896"), use_container_width=True, key="d_sov")
            st.caption(f"Ви: {int(my_mentions)} | Всього: {int(total_mentions)}")

    # Картка 2: Офіційні джерела
    with r1_c2:
        with st.container(border=True):
            st.markdown("<div class='dash-title'>% ОФІЦІЙНИХ ДЖЕРЕЛ</div>", unsafe_allow_html=True)
            st.plotly_chart(make_donut(official_pct, "#36A2EB"), use_container_width=True, key="d_off")
            st.caption(f"Офіційних: {official_sources}")

    # Картка 3: ЗАГАЛЬНИЙ НАСТРІЙ (НОВИЙ ДИЗАЙН)
    with r1_c3:
        with st.container(border=True):
            st.markdown("<div class='dash-title'>ЗАГАЛЬНИЙ НАСТРІЙ</div>", unsafe_allow_html=True)
            
            # 1. Рахуємо
            if not my_brand_rows.empty:
                s_counts = my_brand_rows['sentiment_score'].value_counts()
                pos_count = s_counts.get('Позитивний', 0)
                neu_count = s_counts.get('Нейтральний', 0)
                neg_count = s_counts.get('Негативний', 0)
                total_sent = pos_count + neu_count + neg_count
            else:
                pos_count = neu_count = neg_count = total_sent = 0

            # 2. Відсотки
            pos_pct = (pos_count / total_sent * 100) if total_sent > 0 else 0
            neu_pct = (neu_count / total_sent * 100) if total_sent > 0 else 0
            neg_pct = (neg_count / total_sent * 100) if total_sent > 0 else 0

            # 3. Графік (Смужка)
            fig_sent = go.Figure()
            fig_sent.add_trace(go.Bar(
                y=[''], x=[pos_pct], name='Pos', orientation='h',
                marker=dict(color='#00C896', line=dict(width=0)),
                hovertemplate='%{x:.1f}%<extra></extra>'
            ))
            fig_sent.add_trace(go.Bar(
                y=[''], x=[neu_pct], name='Neu', orientation='h',
                marker=dict(color='#E0E0E0', line=dict(width=0)),
                hovertemplate='%{x:.1f}%<extra></extra>'
            ))
            fig_sent.add_trace(go.Bar(
                y=[''], x=[neg_pct], name='Neg', orientation='h',
                marker=dict(color='#FF4B4B', line=dict(width=0)),
                hovertemplate='%{x:.1f}%<extra></extra>'
            ))

            fig_sent.update_layout(
                barmode='stack', showlegend=False,
                margin=dict(t=0, b=0, l=0, r=0), height=60,
                xaxis=dict(showgrid=False, showticklabels=False, zeroline=False, range=[0, 100]),
                yaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
            )
            
            st.plotly_chart(fig_sent, use_container_width=True, config={'displayModeBar': False})

            # 4. Легенда
            st.markdown(f"""
            <div style="display: flex; justify-content: space-between; font-size: 12px; margin-top: 5px;">
                <div style="text-align: center;"><span style="color:#00C896;">●</span> {pos_pct:.0f}%</div>
                <div style="text-align: center;"><span style="color:#999;">●</span> {neu_pct:.0f}%</div>
                <div style="text-align: center;"><span style="color:#FF4B4B;">●</span> {neg_pct:.0f}%</div>
            </div>
            """, unsafe_allow_html=True)
            
            # Висновок
            main_mood = "Даних немає"
            if total_sent > 0:
                if pos_pct >= max(neu_pct, neg_pct): main_mood = "Переважно Позитивний"
                elif neg_pct >= max(pos_pct, neu_pct): main_mood = "Переважно Негативний"
                else: main_mood = "Переважно Нейтральний"
            
            st.markdown(f"<div style='text-align:center; font-weight:bold; margin-top:5px; font-size:14px; color:#333;'>{main_mood}</div>", unsafe_allow_html=True)

    # === РЯДОК 2 ===
    r2_c1, r2_c2, r2_c3 = st.columns(3)

    with r2_c1:
        with st.container(border=True):
            st.markdown("<div class='dash-title'>СЕРЕДНЯ ПОЗИЦІЯ</div>", unsafe_allow_html=True)
            val_display = f"{avg_pos:.1f}" if avg_pos > 0 else "-"
            st.markdown(f"<div style='text-align:center; font-size: 48px; font-weight: bold; color: #00C896; margin-top: 30px;'>{val_display}</div>", unsafe_allow_html=True)
            
            fig_gauge = go.Figure(go.Indicator(
                mode = "gauge",
                value = avg_pos if avg_pos > 0 else 0,
                domain = {'x': [0, 1], 'y': [0, 1]},
                gauge = {
                    'axis': {'range': [10, 1], 'visible': False}, 
                    'bar': {'color': "#00C896"},
                    'bgcolor': "white",
                }
            ))
            fig_gauge.update_layout(height=100, margin=dict(t=0,b=0,l=20,r=20))
            st.plotly_chart(fig_gauge, use_container_width=True, key="d_pos")

    with r2_c2:
        with st.container(border=True):
            st.markdown("<div class='dash-title'>ПРИСУТНІСТЬ БРЕНДУ</div>", unsafe_allow_html=True)
            st.plotly_chart(make_donut(visibility_rate, "#9966FF"), use_container_width=True, key="d_vis")
            st.caption(f"Знайдено у {scans_with_me} з {total_scans_count} сканувань")

    with r2_c3:
        domain_mentions = 0
        if not df_sources.empty:
            domain_mentions = len(df_sources[df_sources['domain'].str.contains(proj.get('domain', 'MISSING'), na=False, case=False)])
        domain_pct = (domain_mentions / total_sources * 100) if total_sources > 0 else 0.0
        
        with st.container(border=True):
            st.markdown("<div class='dash-title'>ЗГАДКИ ДОМЕНУ</div>", unsafe_allow_html=True)
            st.plotly_chart(make_donut(domain_pct, "#FF9F40"), use_container_width=True, key="d_dom")
            st.caption(f"{domain_mentions} прямих посилань")

    st.markdown("---")

    # 5. ТАБЛИЦЯ
    st.subheader("📋 Деталізація за період")
    
    latest_scans_df = pd.DataFrame(scans_query.data)
    if not latest_scans_df.empty:
        latest_scans_df = latest_scans_df.sort_values('created_at', ascending=False).drop_duplicates('keyword_id')
        
        table_rows = []
        for index, row in latest_scans_df.iterrows():
            kw_text = kw_map.get(row['keyword_id'], "—")
            
            scan_mentions = df_mentions[df_mentions['scan_result_id'] == row['id']]
            my_mention = scan_mentions[scan_mentions['is_my_brand'] == True]
            
            if not my_mention.empty:
                pos = my_mention.iloc[0]['rank_position']
                sent = my_mention.iloc[0]['sentiment_score']
                is_present = True
            else:
                pos = None
                sent = "Не знайдено"
                is_present = False
                
            table_rows.append({
                "Запит": kw_text,
                "Дата": datetime.fromisoformat(row['created_at']).strftime("%d.%m"),
                "Позиція": pos if pos else "-",
                "Тональність": sent,
                "Знайдено?": is_present
            })
            
        st.dataframe(
            pd.DataFrame(table_rows),
            use_container_width=True,
            column_config={
                "Знайдено?": st.column_config.CheckboxColumn("Знайдено?", disabled=True),
            },
            hide_index=True
        )

# =========================
# 7. КЕРУВАННЯ ЗАПИТАМИ
# =========================

def show_keyword_details(kw_id):
    """
    Відображає детальну аналітику по запиту з KPI картками у стилі Virshi (Green/White).
    FIX: Виправлено помилку NoneType при розрахунку rank_position.
    """
    import pandas as pd
    import streamlit as st
    import requests # Потрібно для n8n_trigger_analysis, якщо він викликається тут
    
    # --- 0. ПІДКЛЮЧЕННЯ ---
    if 'supabase' not in globals():
        if 'supabase' in st.session_state:
            supabase = st.session_state['supabase']
        else:
            st.error("🚨 Помилка: Змінна 'supabase' не знайдена.")
            return
    else:
        supabase = globals()['supabase']

    # Локальний мапінг
    MODEL_MAPPING = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }

    # --- 1. ОТРИМАННЯ ДАНИХ ЗАПИТУ ---
    try:
        kw_resp = supabase.table("keywords").select("*").eq("id", kw_id).execute()
        if not kw_resp.data:
            st.error("Запит не знайдено.")
            if st.button("⬅ Назад"):
                st.session_state["focus_keyword_id"] = None
                st.rerun()
            return
        
        keyword_record = kw_resp.data[0]
        keyword_text = keyword_record["keyword_text"]
        project_id = keyword_record["project_id"]
    except Exception as e:
        st.error(f"Помилка БД: {e}")
        return

    # --- 2. HEADER ---
    col_back, col_title = st.columns([1, 10])
    with col_back:
        if st.button("⬅", key="back_main", help="Назад до списку"):
            st.session_state["focus_keyword_id"] = None
            st.rerun()
    
    with col_title:
        st.markdown(f"<h2 style='margin-top: -10px;'>🔍 {keyword_text}</h2>", unsafe_allow_html=True)

    # --- 3. БЛОК УПРАВЛІННЯ ---
    with st.expander("⚙️ Налаштування та Нове сканування", expanded=False):
        c1, c2 = st.columns(2)
        with c1:
            new_text = st.text_input("Редагувати запит", value=keyword_text, key="edit_kw_input")
            if st.button("💾 Зберегти", key="save_kw_btn"):
                if new_text and new_text != keyword_text:
                    supabase.table("keywords").update({"keyword_text": new_text}).eq("id", kw_id).execute()
                    st.success("Збережено!")
                    st.rerun()

        with c2:
            model_choices = list(MODEL_MAPPING.keys())
            selected_models_ui = st.multiselect("Запустити пересканування:", model_choices, default=["Perplexity"], key="rescan_models_select")
            
            if st.button("🚀 Сканувати", key="rescan_btn"):
                if selected_models_ui:
                    proj = st.session_state.get("current_project", {})
                    brand_name = proj.get("brand_name", "MyBrand")
                    with st.spinner(f"Запускаємо {', '.join(selected_models_ui)}..."):
                        # Передбачається, що функція n8n_trigger_analysis доступна глобально
                        success = n8n_trigger_analysis(project_id, [new_text], brand_name, models=selected_models_ui)
                        if success:
                            st.success("Задачу відправлено! Оновіть сторінку за хвилину.")
                else:
                    st.warning("Оберіть хоча б одну ЛЛМ.")

    st.markdown("<br>", unsafe_allow_html=True)

    # --- 4. ЗАВАНТАЖЕННЯ ІСТОРІЇ ---
    try:
        scans_data = (
            supabase.table("scan_results")
            .select("*")
            .eq("keyword_id", kw_id)
            .order("created_at", desc=True)
            .execute()
            .data
        )
    except Exception as e:
        st.error(f"Не вдалося завантажити історію: {e}")
        scans_data = []

    if not scans_data:
        st.info("📭 Для цього запиту ще немає результатів.")
        return

    # --- 5. ВКЛАДКИ ПО МОДЕЛЯХ ---
    tabs = st.tabs(list(MODEL_MAPPING.keys()))

    for tab, ui_model_name in zip(tabs, MODEL_MAPPING.keys()):
        with tab:
            tech_model_id = MODEL_MAPPING[ui_model_name]
            model_scans = [s for s in scans_data if tech_model_id in (s.get("provider") or "").lower()]
            
            if not model_scans:
                st.write(f"📉 Даних від **{ui_model_name}** ще немає.")
                continue

            # Вибір дати
            history_options = {s["created_at"][:16].replace("T", " "): s for s in model_scans}
            col_date, _ = st.columns([2, 4])
            with col_date:
                selected_time = st.selectbox(
                    f"📅 Дата аналізу ({ui_model_name}):", 
                    list(history_options.keys()),
                    key=f"hist_sel_{tech_model_id}"
                )
            
            current_scan = history_options[selected_time]
            scan_id = current_scan["id"]

            # =========================================================
            # 👇 НОВИЙ UI: КАРТКИ KPI
            # =========================================================
            
            # 1. Завантажуємо згадки
            try:
                mentions_kpi = supabase.table("brand_mentions").select("*").eq("scan_result_id", scan_id).execute().data
            except:
                mentions_kpi = []

            # 2. Розрахунок метрик
            total_market_mentions = sum(item.get("mention_count", 0) for item in mentions_kpi) if mentions_kpi else 0
            my_brand_data = next((item for item in mentions_kpi if item.get("is_my_brand") is True), None)

            if my_brand_data:
                val_count = my_brand_data.get("mention_count", 0)
                val_sentiment = my_brand_data.get("sentiment_score", "Нейтральний")
                
                # 🔥 FIX: Обробка NoneType для позиції
                raw_pos = my_brand_data.get("rank_position")
                # Якщо прийшло None -> ставимо 0
                val_position = raw_pos if raw_pos is not None else 0
                
                val_sov = (val_count / total_market_mentions * 100) if total_market_mentions > 0 else 0
            else:
                val_count = 0
                val_sentiment = "Не згадано"
                val_position = 0 
                val_sov = 0

            # Колір для сентименту
            sent_color = "#333"
            if val_sentiment == "Позитивний": sent_color = "#00C896"
            elif val_sentiment == "Негативний": sent_color = "#FF4B4B"
            elif val_sentiment == "Не згадано": sent_color = "#999"

            # 3. HTML/CSS Стилізація
            st.markdown(f"""
            <style>
                .virshi-kpi-container {{
                    display: grid;
                    grid-template-columns: repeat(4, 1fr);
                    gap: 15px;
                    margin-bottom: 25px;
                    font-family: 'Source Sans Pro', sans-serif;
                }}
                .virshi-card {{
                    background-color: white;
                    border: 1px solid #E0E0E0;
                    border-top: 4px solid #00C896; 
                    border-radius: 8px;
                    padding: 20px 15px;
                    text-align: center;
                    box-shadow: 0 4px 6px rgba(0,0,0,0.04);
                    transition: transform 0.2s;
                }}
                .virshi-card:hover {{
                    transform: translateY(-2px);
                    box-shadow: 0 6px 12px rgba(0,0,0,0.08);
                }}
                .virshi-label {{
                    color: #888;
                    font-size: 11px;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                    font-weight: 600;
                    margin-bottom: 10px;
                }}
                .virshi-value {{
                    color: #111;
                    font-size: 28px;
                    font-weight: 700;
                    line-height: 1.2;
                }}
                .virshi-sub {{
                    font-size: 14px;
                    color: {sent_color};
                    font-weight: 600;
                }}
                @media (max-width: 768px) {{
                    .virshi-kpi-container {{ grid-template-columns: repeat(2, 1fr); }}
                }}
            </style>

            <div class="virshi-kpi-container">
                <div class="virshi-card">
                    <div class="virshi-label">Частка Голосу (SOV)</div>
                    <div class="virshi-value">{val_sov:.1f}%</div>
                </div>
                <div class="virshi-card">
                    <div class="virshi-label">Згадок Бренду</div>
                    <div class="virshi-value">{val_count}</div>
                </div>
                <div class="virshi-card">
                    <div class="virshi-label">Тональність</div>
                    <div class="virshi-value virshi-sub">{val_sentiment}</div>
                </div>
                <div class="virshi-card">
                    <div class="virshi-label">Позиція у списку</div>
                    <div class="virshi-value">{val_position if val_position > 0 else "-"}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            # =========================================================
            # ВІДПОВІДЬ ШІ
            # =========================================================
            raw_text = current_scan.get("raw_response", "")
            
            st.markdown("#### 📝 Відповідь ЛЛМ")
            with st.container(border=True):
                if raw_text:
                    my_brand = st.session_state.get("current_project", {}).get("brand_name", "")
                    highlighted_text = raw_text.replace(my_brand, f"<span style='color:#00C896; font-weight:bold;'>{my_brand}</span>")
                    st.markdown(highlighted_text, unsafe_allow_html=True)
                else:
                    st.caption("Текст відповіді не збережено.")

            st.markdown("<br>", unsafe_allow_html=True)

            # =========================================================
            # ТАБЛИЦІ
            # =========================================================
            
            # =========================================================
            # 1. БРЕНДИ (Діаграма замість таблиці)
            # =========================================================
            st.markdown("#### 📊 Конкурентний аналіз (Share of Voice)")
            
            if mentions_kpi:
                import plotly.express as px # Імпортуємо тут, щоб не було помилок
                
                df_brands = pd.DataFrame(mentions_kpi)
                
                # Сортуємо: свій бренд, потім лідери
                df_brands = df_brands.sort_values(by="mention_count", ascending=False)
                
                # Логіка кольорів: Наш = Зелений, Інші = Сірий
                # Створюємо словник кольорів {BrandName: Color}
                color_map = {}
                for index, row in df_brands.iterrows():
                    b_name = row['brand_name']
                    # Якщо це наш бренд - Зелений, інакше - різні відтінки сірого/нейтрального
                    if row.get('is_my_brand'):
                        color_map[b_name] = '#00C896' # Virshi Green
                    else:
                        color_map[b_name] = '#9EA0A5' # Neutral Grey

                # Будуємо "Бублик" (Donut Chart)
                fig_brands = px.pie(
                    df_brands,
                    names='brand_name',
                    values='mention_count',
                    hole=0.6, # Робить "дірку" всередині (бублик)
                    color='brand_name',
                    color_discrete_map=color_map, # Застосовуємо наші кольори
                    hover_data=['rank_position']
                )

                # Налаштування вигляду
                fig_brands.update_traces(
                    textposition='inside', 
                    textinfo='percent+label',
                    hovertemplate = "<b>%{label}</b><br>Згадок: %{value}<br>Частка: %{percent}"
                )
                
                fig_brands.update_layout(
                    showlegend=False, # Ховаємо легенду, щоб не забивати місце (підписи всередині)
                    margin=dict(t=0, b=0, l=0, r=0),
                    height=300
                )

                # Відображаємо
                c_chart, c_table = st.columns([1.5, 1])
                
                with c_chart:
                    st.plotly_chart(fig_brands, use_container_width=True)
                
                # Додатково маленька легенда/таблиця справа для точності
                with c_table:
                    st.markdown("**Топ лідерів:**")
                    # Проста табличка топ-5
                    top_df = df_brands[['brand_name', 'mention_count', 'rank_position']].head(5)
                    st.dataframe(
                        top_df, 
                        use_container_width=True, 
                        hide_index=True,
                        column_config={
                            "brand_name": "Бренд",
                            "mention_count": st.column_config.NumberColumn("Згадок"),
                            "rank_position": st.column_config.NumberColumn("Ранг")
                        }
                    )

            else:
                st.info("Брендів у відповіді не знайдено.")

            st.markdown("<br>", unsafe_allow_html=True)

          # =========================================================
            # 2. ДЖЕРЕЛА (Безпечний вивід)
            # =========================================================
            st.markdown("#### 🔗 Цитовані джерела")
            
            try:
                sources_resp = (
                    supabase.table("extracted_sources")
                    .select("*")
                    .eq("scan_result_id", scan_id)
                    .execute()
                )
                sources_data = sources_resp.data

                if sources_data:
                    df_src = pd.DataFrame(sources_data)
                    
                    # 🔥 FIX: Гарантуємо наявність колонок перед зверненням
                    if 'url' not in df_src.columns: df_src['url'] = None
                    if 'domain' not in df_src.columns: df_src['domain'] = "-"
                    if 'is_official' not in df_src.columns: df_src['is_official'] = False
                    if 'mention_count' not in df_src.columns: df_src['mention_count'] = 1

                    # Очищення
                    df_src['url'] = df_src['url'].fillna("#")
                    df_src['mention_count'] = df_src['mention_count'].fillna(1).astype(int)

                    # Статус
                    df_src['Статус'] = df_src['is_official'].apply(lambda x: "✅ Офіційне" if x is True else "🔗 Зовнішнє")
                    
                    # Сортування
                    df_src = df_src.sort_values(by=['mention_count'], ascending=False)

                    # Відображення (тільки існуючі колонки)
                    st.dataframe(
                        df_src[['url', 'Статус', 'mention_count']], 
                        use_container_width=True, 
                        hide_index=True,
                        column_config={
                            "url": st.column_config.LinkColumn(
                                "Посилання (URL)",
                                width="large",
                                validate="^https?://", 
                            ),
                            "Статус": st.column_config.TextColumn("Тип", width="small"),
                            "mention_count": st.column_config.NumberColumn("Згадок", format="%d", width="small")
                        }
                    )
                else:
                    st.info("ℹ️ Джерел не знайдено.")
                    
            except Exception as e:
                st.error(f"⚠️ Помилка таблиці джерел: {e}")


def show_keywords_page():
    """
    Сторінка списку запитів.
    Дизайн: Картки (Контейнери).
    Функціонал: Деталі, Крон-перемикач, Масовий запуск, Динамічне додавання.
    """
    import pandas as pd
    import streamlit as st
    from datetime import datetime
    import time

    # Локальний мапінг
    MODEL_MAPPING = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }

    # Ініціалізація лічильника інпутів
    if "kw_input_count" not in st.session_state:
        st.session_state["kw_input_count"] = 1

    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект в онбордингу.")
        return

    # Якщо натиснули "Деталі" - показуємо іншу сторінку
    if st.session_state.get("focus_keyword_id"):
        show_keyword_details(st.session_state["focus_keyword_id"])
        return

    st.title("📋 Перелік запитів")

    # ========================================================
    # 1. БЛОК ДОДАВАННЯ (ЗГОРНУТИЙ ЗА ЗАМОВЧУВАННЯМ)
    # ========================================================
    with st.expander("➕ Додати нові запити", expanded=False): # <--- Згорнуто
        with st.container(border=True):
            st.markdown("##### 📝 Введіть нові запити")
            
            # Динамічні поля
            for i in range(st.session_state["kw_input_count"]):
                st.text_input(f"Запит #{i+1}", key=f"new_kw_input_{i}", placeholder="Наприклад: Купити квитки...")

            # Кнопки +/-
            col_plus, col_minus, _ = st.columns([1, 1, 5])
            with col_plus:
                if st.button("➕ Ще рядок"):
                    st.session_state["kw_input_count"] += 1
                    st.rerun()
            with col_minus:
                if st.session_state["kw_input_count"] > 1:
                    if st.button("➖ Прибрати"):
                        st.session_state["kw_input_count"] -= 1
                        st.rerun()

            st.divider()

            # Вибір ЛЛМ і Сабміт
            c_models, c_submit = st.columns([3, 1])
            with c_models:
                selected_models_add = st.multiselect("LLM для першого скану:", list(MODEL_MAPPING.keys()), default=["Perplexity"], key="add_multiselect")
            
            with c_submit:
                st.write("")
                st.write("")
                if st.button("🚀 Додати", use_container_width=True, type="primary"):
                    new_keywords_list = []
                    for i in range(st.session_state["kw_input_count"]):
                        val = st.session_state.get(f"new_kw_input_{i}", "").strip()
                        if val: new_keywords_list.append(val)
                    
                    if new_keywords_list:
                        try:
                            # Вставляємо в базу
                            insert_data = [{"project_id": proj["id"], "keyword_text": kw, "is_active": True, "is_cron_active": False} for kw in new_keywords_list]
                            res = supabase.table("keywords").insert(insert_data).execute()
                            
                            if res.data:
                                # Запускаємо скан
                                with st.spinner(f"Запускаємо аналіз..."):
                                    n8n_trigger_analysis(proj["id"], new_keywords_list, proj.get("brand_name"), models=selected_models_add)
                                
                                st.success(f"Додано {len(new_keywords_list)} запитів!")
                                # Скидаємо поля
                                st.session_state["kw_input_count"] = 1
                                for key in list(st.session_state.keys()):
                                    if key.startswith("new_kw_input_"): del st.session_state[key]
                                time.sleep(1)
                                st.rerun()
                        except Exception as e:
                            st.error(f"Помилка: {e}")
                    else:
                        st.warning("Введіть хоча б один запит.")

    st.divider()
    
    # ========================================================
    # 2. ОТРИМАННЯ ДАНИХ
    # ========================================================
    try:
        # Запити
        keywords = supabase.table("keywords").select("*").eq("project_id", proj["id"]).execute().data
        
        # Дати останнього скану
        last_scans_resp = supabase.table("scan_results")\
            .select("keyword_id, created_at")\
            .eq("project_id", proj["id"])\
            .order("created_at", desc=True)\
            .execute()
            
        last_scan_map = {}
        if last_scans_resp.data:
            for s in last_scans_resp.data:
                if s['keyword_id'] not in last_scan_map:
                    last_scan_map[s['keyword_id']] = s['created_at']
        
        # Збагачуємо даними
        for k in keywords:
            k['last_scan_date'] = last_scan_map.get(k['id'], "1970-01-01T00:00:00+00:00")

    except Exception as e:
        st.error(f"Помилка завантаження: {e}")
        keywords = []

    if not keywords:
        st.info("Список порожній.")
        return

    # ========================================================
    # 3. ПАНЕЛЬ УПРАВЛІННЯ (Сортування та Масові дії)
    # ========================================================
    
    # --- Рядок 1: Сортування ---
    c_sort, _ = st.columns([2, 4])
    with c_sort:
        sort_option = st.selectbox(
            "Сортувати за:", 
            ["Найновіші (Додані)", "Найстаріші (Додані)", "Нещодавно проскановані", "Давно не скановані"],
            label_visibility="collapsed"
        )

    # Логіка сортування
    if sort_option == "Найновіші (Додані)":
        keywords.sort(key=lambda x: x['created_at'], reverse=True)
    elif sort_option == "Найстаріші (Додані)":
        keywords.sort(key=lambda x: x['created_at'], reverse=False)
    elif sort_option == "Нещодавно проскановані":
        keywords.sort(key=lambda x: x['last_scan_date'], reverse=True)
    elif sort_option == "Давно не скановані":
        keywords.sort(key=lambda x: x['last_scan_date'], reverse=False)

# --- Рядок 2: Масові дії (Container) ---
    with st.container(border=True):
        c_check, c_models, c_btn = st.columns([0.5, 3, 1.5])
        
        with c_check:
            st.write("") 
            select_all = st.checkbox("Всі", key="select_all_kws")
        
        with c_models:
            bulk_models = st.multiselect(
                "ЛЛМ для запуску:", 
                list(MODEL_MAPPING.keys()), 
                default=["Perplexity"], 
                label_visibility="collapsed",
                key="bulk_models_main"
            )
        
        with c_btn:
            if st.button("🚀 Аналізувати обрані", use_container_width=True):
                # Збираємо ID
                selected_kws_text = []
                if select_all:
                    selected_kws_text = [k['keyword_text'] for k in keywords]
                else:
                    for k in keywords:
                        if st.session_state.get(f"chk_{k['id']}", False):
                            selected_kws_text.append(k['keyword_text'])
                
                if selected_kws_text:
                    with st.spinner(f"Відправляємо {len(selected_kws_text)} запитів..."):
                        n8n_trigger_analysis(proj["id"], selected_kws_text, proj.get("brand_name"), models=bulk_models)
                        st.success("Успішно! Оновіть сторінку за хвилину.")
                        
                        # 🔥 FIX: ВИДАЛЕНО РЯДОК, ЩО ВИКЛИКАВ ПОМИЛКУ
                        # if select_all: st.session_state["select_all_kws"] = False <--- ЦЕ БУЛА ПРИЧИНА
                        
                        time.sleep(2)
                        st.rerun()
                else:
                    st.warning("Оберіть хоча б один запит.")

    # ========================================================
    # 4. СПИСОК ЗАПИТІВ (КАРТКИ)
    # ========================================================
    
    # Заголовки стовпчиків (для краси)
    h1, h2, h3, h4, h5 = st.columns([0.5, 3, 1.5, 1.5, 1.2])
    h2.markdown("**Запит**")
    h3.markdown("**⏰ Авто-Скан (CRON)**")
    h4.markdown("**Останній аналіз**")
    h5.markdown("**Дії**")

    # Функція оновлення крону (Callback)
    def update_cron_status(kw_id, new_status):
        try:
            supabase.table("keywords").update({"is_cron_active": new_status}).eq("id", kw_id).execute()
            # Можна додати toast повідомлення
        except Exception as e:
            st.error(f"Error updating cron: {e}")

    # Вивід рядків
    for k in keywords:
        with st.container(border=True):
            c1, c2, c3, c4, c5 = st.columns([0.5, 3, 1.5, 1.5, 1.2])
            
            # 1. Чекбокс вибору
            with c1:
                is_checked = select_all
                st.checkbox("", key=f"chk_{k['id']}", value=is_checked)
            
            # 2. Текст запиту
            with c2:
                st.markdown(f"**{k['keyword_text']}**")
            
            # 3. CRON Перемикач (Toggle)
            with c3:
                # Використовуємо toggle. Ключ унікальний для кожного рядка
                cron_active = k.get('is_cron_active', False)
                new_cron = st.toggle(
                    "Увімк.", 
                    value=cron_active, 
                    key=f"cron_{k['id']}",
                    label_visibility="collapsed"
                )
                
                # Якщо значення змінилося в UI, оновлюємо БД
                if new_cron != cron_active:
                    update_cron_status(k['id'], new_cron)
                    # Опціонально: st.rerun() якщо хочемо миттєве оновлення даних, 
                    # але краще без нього для плавності, дані оновляться при наступній дії.
            
            # 4. Дата
            with c4:
                date_iso = k.get('last_scan_date')
                if date_iso and date_iso != "1970-01-01T00:00:00+00:00":
                    dt_obj = datetime.fromisoformat(date_iso.replace('Z', '+00:00'))
                    formatted_date = dt_obj.strftime("%d.%m %H:%M")
                    st.caption(f"🕒 {formatted_date}")
                else:
                    st.caption("—")
            
            # 5. Дії (Деталі + Видалити)
            with c5:
                b1, b2 = st.columns(2)
                # Кнопка Деталі
                if b1.button("🔍", key=f"det_{k['id']}", help="Детальний аналіз"):
                    st.session_state["focus_keyword_id"] = k["id"]
                    st.rerun()
                
                # Кнопка Видалити
                if b2.button("🗑", key=f"del_{k['id']}", help="Видалити"):
                    supabase.table("keywords").delete().eq("id", k["id"]).execute()
                    st.rerun()

# =========================
# 8. РЕКОМЕНДАЦІЇ
# =========================


def show_recommendations_page():
    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект.")
        return

    st.title("💡 Центр Стратегій та Дій")
    st.caption("ШІ-аналітик аналізує ваші позиції та генерує покроковий план дій.")

    # 1. Розділяємо на типи рекомендацій
    # Ми використовуємо константи, які співпадають з ENUM в базі даних (rec_type)
    tabs = st.tabs(["📣 PR Стратегія", "💻 Digital & SEO", "✍️ Контент-план", "📱 Social Media"])
    types = ["pr", "digital", "content", "social"]

    for tab, r_type in zip(tabs, types):
        with tab:
            # --- Блок Генерації (Тільки для Адміна або якщо дозволено юзеру) ---
            # Можна додати перевірку: if st.session_state["role"] == "admin":
            
            with st.container(border=True):
                c1, c2 = st.columns([3, 1])
                with c1:
                    st.markdown(f"**Згенерувати новий звіт: {r_type.upper()}**")
                    st.caption("Аналіз останніх 30 днів, пошук розривів (gaps) та план дій.")
                with c2:
                    if st.button(f"🚀 Запустити AI", key=f"btn_gen_{r_type}"):
                        with st.spinner("Gemini аналізує дані та пише звіт... Це може зайняти до 30 секунд."):
                            try:
                                # Тут ми викликаємо n8n вебхук
                                # Для MVP поки що робимо запис-заглушку, якщо n8n не підключений
                                # АБО викликаємо реальну функцію n8n_request_recommendations
                                
                                # Варіант А: Реальний виклик (розкоментуйте, коли буде готовий n8n)
                                # n8n_request_recommendations(proj, r_type, "Auto-generated report")
                                
                                # Варіант Б: Симуляція (щоб ви побачили як це виглядає зараз)
                                fake_report = f"""
                                # Стратегія {r_type.upper()} для {proj.get('brand_name')}
                                **Дата:** {datetime.now().strftime('%Y-%m-%d')}
                                
                                ## 1. Аналіз ситуації
                                Наразі частка голосу (SOV) складає **{proj.get('sov', '15')}%**.
                                Основні конкуренти домінують у категорії "Депозити".
                                
                                ## 2. Ключові проблеми
                                * Відсутність згадок на *Minfin.com.ua*.
                                * Низька тональність у відповідях Perplexity.
                                
                                ## 3. План дій (Action Items)
                                1. **Стаття-огляд:** Замовити розміщення на finance.ua.
                                2. **Робота з відгуками:** Відповісти на скарги на форумах.
                                """
                                
                                supabase.table("recommendation_reports").insert({
                                    "project_id": proj["id"],
                                    "report_type": r_type,
                                    "report_content": fake_report
                                }).execute()
                                
                                st.success("Звіт успішно згенеровано!")
                                time.sleep(1)
                                st.rerun()
                                
                            except Exception as e:
                                st.error(f"Помилка генерації: {e}")

            st.divider()
            st.subheader("📂 Історія звітів")

            # 2. Виведення історії звітів з бази
            try:
                reports = (
                    supabase.table("recommendation_reports")
                    .select("*")
                    .eq("project_id", proj["id"])
                    .eq("report_type", r_type)
                    .order("created_at", desc=True)
                    .execute()
                    .data
                )
            except Exception as e:
                st.error(f"Помилка завантаження: {e}")
                reports = []

            if not reports:
                st.info("У цій категорії ще немає звітів.")
            else:
                for rep in reports:
                    date_str = str(rep['created_at'])[:10]
                    with st.expander(f"📄 Звіт від {date_str}"):
                        # Кнопка видалення звіту
                        if st.button("Видалити звіт", key=f"del_rep_{rep['id']}"):
                            supabase.table("recommendation_reports").delete().eq("id", rep['id']).execute()
                            st.rerun()
                        
                        st.markdown("---")
                        # Рендеринг Markdown (основна фішка)
                        st.markdown(rep['report_content'])

# =========================
# 9. SIDEBAR
# =========================
def show_sources_page():
    """
    Сторінка управління джерелами та аналізу репутації.
    Оновлено: 
    - Глобальні фільтри зверху.
    - Фільтр LLM через чекбокси.
    - Фільтр по Запитах (Dropdown).
    - Об'єднання даних для фільтрації.
    """
    import pandas as pd
    import plotly.express as px
    import streamlit as st
    import time
    
    # 0. ПІДКЛЮЧЕННЯ
    if 'supabase' not in globals():
        if 'supabase' in st.session_state:
            supabase = st.session_state['supabase']
        else:
            st.error("🚨 Помилка: Змінна 'supabase' не знайдена.")
            return
    else:
        supabase = globals()['supabase']

    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект.")
        return

    MODEL_MAPPING = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }
    ALL_MODELS_KEYS = list(MODEL_MAPPING.keys())

    st.title("📡 Джерела та Репутація")
    
    # === 1. ЗАВАНТАЖЕННЯ ТА ОБ'ЄДНАННЯ ДАНИХ ===
    # Нам потрібно знати Keyword і Provider для кожного джерела, 
    # тому ми тягнемо все і мерджимо.
    try:
        # A. Whitelist
        assets_resp = supabase.table("official_assets").select("*").eq("project_id", proj["id"]).order("created_at", desc=True).execute()
        assets = assets_resp.data if assets_resp.data else []
        whitelist = [a['domain_or_url'] for a in assets]

        # B. Скани (метадані)
        scans_resp = supabase.table("scan_results").select("id, provider, keyword_id").eq("project_id", proj["id"]).execute()
        if not scans_resp.data:
            st.info("Даних немає.")
            return
        df_scans = pd.DataFrame(scans_resp.data)

        # C. Ключові слова
        kws_resp = supabase.table("keywords").select("id, keyword_text").eq("project_id", proj["id"]).execute()
        kw_map = {k['id']: k['keyword_text'] for k in kws_resp.data}
        
        # D. Джерела
        scan_ids = df_scans['id'].tolist()
        sources_resp = supabase.table("extracted_sources").select("*").in_("scan_result_id", scan_ids).execute()
        df_sources = pd.DataFrame(sources_resp.data)

        if df_sources.empty:
            df_full = pd.DataFrame()
        else:
            # E. MERGE (Джерела + Скани + Слова)
            # Додаємо keyword_text до scans
            df_scans['keyword_text'] = df_scans['keyword_id'].map(kw_map)
            
            # Додаємо інфо про скан до джерел
            df_full = pd.merge(df_sources, df_scans, left_on='scan_result_id', right_on='id', how='left')

            # Чистка
            if 'domain' not in df_full.columns: df_full['domain'] = None
            if 'url' not in df_full.columns: df_full['url'] = None
            if 'is_official' not in df_full.columns: df_full['is_official'] = False
            df_full['is_official'] = df_full['is_official'].fillna(False)

    except Exception as e:
        st.error(f"Помилка завантаження даних: {e}")
        return

    # === 2. ГЛОБАЛЬНІ ФІЛЬТРИ ===
    with st.container(border=True):
        st.markdown("**⚙️ Фільтри відображення**")
        
        # Ряд 1: Чекбокси LLM
        c_llm_label, c_llm_opts = st.columns([1, 4])
        with c_llm_label:
            st.caption("Оберіть моделі:")
        
        with c_llm_opts:
            # Створюємо чекбокси горизонтально
            cols = st.columns(len(ALL_MODELS_KEYS))
            selected_models = []
            for i, model_name in enumerate(ALL_MODELS_KEYS):
                # За дефолтом всі обрані
                if cols[i].checkbox(model_name, value=True, key=f"chk_src_{model_name}"):
                    selected_models.append(MODEL_MAPPING[model_name])
        
        # Ряд 2: Dropdown Запитів
        all_keywords = df_full['keyword_text'].dropna().unique().tolist() if not df_full.empty else []
        selected_keywords = st.multiselect(
            "Фільтр по запитах:",
            options=all_keywords,
            default=all_keywords,
            placeholder="Оберіть запити для аналізу..."
        )

    # === 3. ФІЛЬТРАЦІЯ ДАНИХ ===
    if not df_full.empty:
        # 1. Фільтр по моделях
        mask_model = df_full['provider'].apply(lambda x: any(t in str(x) for t in selected_models))
        # 2. Фільтр по словах
        mask_kw = df_full['keyword_text'].isin(selected_keywords)
        
        df_filtered = df_full[mask_model & mask_kw].copy()
    else:
        df_filtered = pd.DataFrame()

    if df_filtered.empty:
        st.warning("За обраними фільтрами даних немає.")
        return

    # === 4. ВКЛАДКИ ===
    st.write("")
    tab1, tab2, tab3 = st.tabs(["🛡️ Офіційні ресурси бренду", "🌐 Ренкінг доменів", "🔗 Посилання"])

    # -------------------------------------------------------
    # TAB 1: ОФІЦІЙНІ РЕСУРСИ (Статистика + Редагування)
    # -------------------------------------------------------
    with tab1:
        st.markdown("##### 📊 Аналіз охоплення офіційних ресурсів")
        
        # Групування: Офіційні vs Зовнішні
        df_filtered['Тип'] = df_filtered['is_official'].apply(lambda x: "✅ Офіційні" if x else "🔗 Зовнішні")
        stats_tab1 = df_filtered['Тип'].value_counts().reset_index()
        stats_tab1.columns = ['Тип', 'Кількість']
        
        # Графік
        c_chart, c_stat = st.columns([1, 1])
        with c_chart:
            if not stats_tab1.empty:
                fig_official = px.pie(
                    stats_tab1, 
                    names='Тип', 
                    values='Кількість', 
                    hole=0.6,
                    color='Тип',
                    color_discrete_map={"✅ Офіційні": "#00C896", "🔗 Зовнішні": "#E0E0E0"}
                )
                fig_official.update_traces(textinfo='percent+label')
                fig_official.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), height=250)
                st.plotly_chart(fig_official, use_container_width=True)
        
        with c_stat:
            st.markdown("**Статистика (за фільтром):**")
            total_links = stats_tab1['Кількість'].sum()
            off_links = df_filtered[df_filtered['is_official']==True].shape[0]
            st.metric("Всього знайдено посилань", total_links)
            st.metric("З них на ваші ресурси", off_links)

        st.divider()
        st.markdown("##### ⚙️ Керування списком (Whitelist)")
        
        # Блок додавання активів
        with st.container(border=True):
            c1, c2, c3 = st.columns([3, 1, 1])
            with c1:
                new_asset = st.text_input("URL або Домен", placeholder="example.com", key="add_new_asset_input")
            with c2:
                asset_type = st.selectbox("Тип", ["website", "social", "article"], label_visibility="visible", key="add_new_asset_type")
            with c3:
                st.write("") 
                st.write("") 
                if st.button("➕ Додати", use_container_width=True):
                    if new_asset:
                        try:
                            clean = new_asset.replace("https://", "").replace("http://", "").strip().rstrip("/")
                            supabase.table("official_assets").insert({
                                "project_id": proj["id"], "domain_or_url": clean, "type": asset_type
                            }).execute()
                            st.success("Додано!")
                            time.sleep(0.5)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Помилка: {e}")

        if assets:
            st.caption("Ваші активи (можна редагувати):")
            for asset in assets:
                edit_key = f"edit_mode_{asset['id']}"
                with st.container(border=True):
                    if st.session_state.get(edit_key, False):
                        # РЕЖИМ РЕДАГУВАННЯ
                        ec1, ec2 = st.columns([4, 1])
                        with ec1:
                            new_val = st.text_input("Редагування", value=asset['domain_or_url'], key=f"input_{asset['id']}", label_visibility="collapsed")
                        with ec2:
                            b_save, b_cancel = st.columns(2)
                            if b_save.button("💾", key=f"save_{asset['id']}", help="Зберегти"):
                                try:
                                    clean_val = new_val.replace("https://", "").replace("http://", "").strip().rstrip("/")
                                    supabase.table("official_assets").update({"domain_or_url": clean_val}).eq("id", asset['id']).execute()
                                    st.session_state[edit_key] = False
                                    st.success("Збережено!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Помилка: {e}")
                            if b_cancel.button("❌", key=f"cancel_{asset['id']}", help="Скасувати"):
                                st.session_state[edit_key] = False
                                st.rerun()
                    else:
                        # РЕЖИМ ПЕРЕГЛЯДУ
                        c_txt, c_type, c_acts = st.columns([3.5, 1, 1.5])
                        with c_txt:
                            st.markdown(f"**{asset['domain_or_url']}**")
                        with c_type:
                            st.caption(asset['type'].upper())
                        with c_acts:
                            b_edit, b_del = st.columns(2)
                            if b_edit.button("✏️", key=f"edit_btn_{asset['id']}"):
                                st.session_state[edit_key] = True
                                st.rerun()
                            if b_del.button("🗑", key=f"del_{asset['id']}"):
                                supabase.table("official_assets").delete().eq("id", asset['id']).execute()
                                st.rerun()
        else:
            st.info("Список пустий. Додайте ваш сайт.")

    # -------------------------------------------------------
    # TAB 2: РЕНКІНГ ДОМЕНІВ
    # -------------------------------------------------------
    with tab2:
        st.markdown(f"##### 🏆 Топ Доменів")
        
        if not df_filtered.empty and df_filtered['domain'].notna().any():
            df_tab2 = df_filtered.copy()
            df_tab2['domain'] = df_tab2['domain'].astype(str)
            
            domain_stats = df_tab2.groupby('domain').agg(
                Mentions=('id', 'count'),
                Queries=('scan_result_id', 'nunique')
            ).reset_index().sort_values('Mentions', ascending=False)

            def check_off(d): return any(w in str(d) for w in whitelist)
            domain_stats['Type'] = domain_stats['domain'].apply(lambda x: "✅ Офіційне" if check_off(x) else "🔗 Зовнішнє")
            
            st.dataframe(
                domain_stats, 
                use_container_width=True,
                column_config={
                    "domain": "Домен",
                    "Type": "Тип",
                    "Mentions": st.column_config.ProgressColumn("Цитувань", format="%d", min_value=0, max_value=int(domain_stats['Mentions'].max())),
                    "Queries": "Охоплення запитів"
                },
                hide_index=True
            )
        else:
            st.info("Доменів не знайдено.")

    # -------------------------------------------------------
    # TAB 3: ПОСИЛАННЯ (Повні URL + Графік)
    # -------------------------------------------------------
    with tab3:
        st.markdown("##### 🔗 Топ Конкретних Посилань")
        
        if not df_filtered.empty and df_filtered['url'].notna().any():
            df_urls = df_filtered[df_filtered['url'].notna() & (df_filtered['url'] != "")].copy()
            
            if not df_urls.empty:
                df_urls['url'] = df_urls['url'].astype(str)
                
                # Групування
                url_stats = df_urls.groupby('url').agg(
                    Mentions=('id', 'count')
                ).reset_index().sort_values('Mentions', ascending=False)
                
                # Додаємо скорочений URL для графіка
                url_stats['ShortURL'] = url_stats['url'].apply(lambda x: x[:40] + "..." if len(x) > 40 else x)

                # Графік (Бублик Топ-10)
                c_chart, c_table = st.columns([1, 1.5])
                
                with c_chart:
                    st.markdown("**Топ-10 посилань:**")
                    top_10 = url_stats.head(10)
                    if not top_10.empty:
                        fig_urls = px.pie(
                            top_10,
                            names='ShortURL',
                            values='Mentions',
                            hole=0.6,
                        )
                        fig_urls.update_traces(textposition='inside', textinfo='percent')
                        fig_urls.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), height=250)
                        st.plotly_chart(fig_urls, use_container_width=True)

                with c_table:
                    st.markdown("**Детальний список:**")
                    st.dataframe(
                        url_stats.head(100),
                        use_container_width=True,
                        column_config={
                            "url": st.column_config.LinkColumn(
                                "Повне Посилання",
                                display_text=r"https?://.*", # Показувати повний текст URL
                                width="large"
                            ),
                            "Mentions": st.column_config.NumberColumn("К-сть цитувань", format="%d"),
                            "ShortURL": None
                        },
                        hide_index=True
                    )
            else:
                st.info("URL-адреси відсутні.")
        else:
            st.info("Немає даних URL.")

def sidebar_menu():
    from streamlit_option_menu import option_menu
    
    # Отримуємо дані
    user = st.session_state.get("user")
    role = st.session_state.get("role", "user")
    current_proj = st.session_state.get("current_project")

    # --- 🎨 CSS ДЛЯ АДМІНА (Заливка сайдбару) ---
    if role == "admin":
        st.markdown("""
        <style>
            [data-testid="stSidebar"] {
                background-color: #E8F5E9; /* Світло-зелений фон для Адміна */
                border-right: 2px solid #00C896; /* Акцентна лінія справа */
            }
            /* Можна підфарбувати заголовки, щоб було стильно */
            [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3 {
                color: #00695C;
            }
        </style>
        """, unsafe_allow_html=True)

    with st.sidebar:
        # 1. ЛОГОТИП
        st.image("https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png", width=150) 
        #st.markdown("## AI Visibility by Virshi") 

        # Профіль
        user_name = "Користувач"
        if user:
            meta = user.user_metadata
            user_name = meta.get("full_name") or meta.get("name") or user.email.split("@")[0]

        st.caption(f"👤 {user_name}")
        
        # ❌ ТУТ ПРИБРАЛИ НАПИС "Admin Mode"
        
        st.divider()

        # 2. ВИБІР ПРОЕКТУ
        if role == "admin":
            try:
                if 'supabase' in globals():
                    projs_resp = supabase.table("projects").select("id, brand_name, status").execute()
                    projects_list = projs_resp.data
                else:
                    projects_list = []

                options_map = {f"{p['brand_name']} (ID: {p['id']})": p for p in projects_list}
                
                current_index = 0
                if current_proj:
                    current_key = f"{current_proj['brand_name']} (ID: {current_proj['id']})"
                    if current_key in options_map:
                        current_index = list(options_map.keys()).index(current_key)

                selected_key = st.selectbox(
                    "📂 Оберіть проект:",
                    options=list(options_map.keys()),
                    index=current_index,
                    placeholder="Пошук по Назві або ID...",
                    help="Введіть ID для пошуку"
                )

                if selected_key:
                    new_proj = options_map[selected_key]
                    if not current_proj or new_proj['id'] != current_proj['id']:
                        st.session_state["current_project"] = new_proj
                        st.rerun()

            except Exception as e:
                st.error(f"Error: {e}")

        else:
            # ЮЗЕР
            if current_proj:
                st.markdown(f"### 📂 {current_proj.get('brand_name')}")
                with st.expander("ℹ️ Project ID"):
                    st.code(current_proj.get('id'), language=None)
            else:
                st.warning("Проект не обрано")

        st.write("") 

    # 3. НАВІГАЦІЯ
    with st.sidebar:
        selected = option_menu(
            "Меню",
            ["Дашборд", "Перелік запитів", "Джерела", "Конкуренти", "Рекомендації", "GPT-Visibility", "Адмін"] if role == "admin" else ["Дашборд", "Перелік запитів", "Джерела", "Конкуренти", "Рекомендації", "GPT-Visibility"],
            icons=["speedometer2", "list-task", "router", "people", "lightbulb", "robot", "shield-lock"],
            menu_icon="cast",
            default_index=0,
            styles={
                "nav-link-selected": {"background-color": "#00C896"}, 
            }
        )

    # 4. ФУТЕР
    with st.sidebar:
        st.divider()
        
        # Статус плану
        if st.session_state.get("current_project"):
            status_text = st.session_state["current_project"].get("status", "TRIAL").upper()
            color = "#FFA500" if "TRIAL" in status_text else "#00C896"
            st.markdown(f"Статус: <span style='color:{color}; font-weight:bold;'>● {status_text}</span>", unsafe_allow_html=True)
        
        st.write("")
        
        # ✅ ТУТ ДОДАЛИ НАПИС "Admin Mode" (Тільки для адміна)
        if role == "admin":
            st.caption("🛡️ Admin Mode")

        # Support
        st.caption("Support: hi@virshi.ai")

        # Кнопка Виходу
        if st.button("Вийти з акаунту", key="logout_btn", use_container_width=True):
            logout()

    return selected

def show_auth_page():
    """
    Renders the centered authentication card (Login / Register) with Virshi styling.
    """
    # Apply custom CSS for the auth page
    st.markdown("""
    <style>
        /* General Page Background */
        .stApp {
            background-color: #F4F7F6;
        }
        
        /* Center the form container */
        [data-testid="stForm"] {
            background-color: #ffffff;
            padding: 40px;
            border-radius: 12px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.08);
            border: 1px solid #EAEAEA;
        }

        /* Input fields styling */
        .stTextInput > div > div > input {
            border-radius: 8px;
            border: 1px solid #e0e0e0;
            padding: 10px;
        }

        /* Primary Button (Virshi Green) */
        .stButton > button {
            width: 100%;
            background-color: #00C896 !important;
            color: white !important;
            border: none;
            border-radius: 8px;
            padding: 12px;
            font-weight: 600;
            margin-top: 10px;
        }
        .stButton > button:hover {
            background-color: #00a87e !important;
        }
        
        /* Tabs Styling */
        .stTabs [data-baseweb="tab-list"] {
            gap: 20px;
            justify-content: center;
        }
        .stTabs [data-baseweb="tab"] {
            height: 50px;
            white-space: pre-wrap;
            border-radius: 4px 4px 0 0;
            gap: 1px;
            padding-top: 10px;
            padding-bottom: 10px;
        }
    </style>
    """, unsafe_allow_html=True)

    # Centering Layout using Columns
    # [Empty Left] [Center Card] [Empty Right]
    col_l, col_center, col_r = st.columns([1, 1.5, 1])

    with col_center:
        # Logo Section
        st.markdown(
            '<div style="text-align: center; margin-bottom: 20px;">'
            '<img src="https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png" width="180">'
            '</div>',
            unsafe_allow_html=True,
        )
        
        st.markdown("<h3 style='text-align: center; color: #333; margin-bottom: 5px;'>Welcome to Virshi</h3>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: center; color: #666; margin-bottom: 30px;'>Sign in to manage your AI visibility</p>", unsafe_allow_html=True)

        # Tabs for Login / Register
        tab_login, tab_register = st.tabs(["🔑 Sign In", "📝 Sign Up"])

        # --- LOGIN TAB ---
        with tab_login:
            with st.form("login_form"):
                email = st.text_input("Email", placeholder="name@company.com")
                password = st.text_input("Password", type="password", placeholder="••••••••")
                
                st.write("") # Spacer
                
                submit = st.form_submit_button("Sign In", use_container_width=True)
                
                if submit:
                    if not email or not password:
                        st.warning("Please fill in all fields.")
                    else:
                        login_user(email, password)

        # --- REGISTER TAB ---
        with tab_register:
            with st.form("register_form"):
                c1, c2 = st.columns(2)
                with c1:
                    first_name = st.text_input("First Name", placeholder="Ivan")
                with c2:
                    last_name = st.text_input("Last Name", placeholder="Petrenko")
                
                new_email = st.text_input("Email", placeholder="name@company.com")
                new_password = st.text_input("Password", type="password", placeholder="••••••••", help="Min 6 chars")
                
                st.write("") # Spacer
                
                submit_reg = st.form_submit_button("Create Account", use_container_width=True)
                
                if submit_reg:
                    if not new_email or not new_password or not first_name:
                        st.warning("Please fill in required fields.")
                    elif len(new_password) < 6:
                        st.warning("Password must be at least 6 characters.")
                    else:
                        register_user(new_email, new_password, first_name, last_name)



def show_admin_page():
    """
    Повноцінна CRM для Адміністратора.
    Виправлено: помилка int64 (JSON serialization), відображення Email, порядок вкладок.
    """
    import pandas as pd
    import streamlit as st
    import time
    
    # Перевірка доступу
    if st.session_state.get("role") != "admin":
        st.error("⛔ Доступ заборонено.")
        return

    st.title("🛡️ Admin Panel (CRM)")
    
    # Стилізація метрик
    st.markdown("""
    <style>
        .metric-box {
            background-color: #F8F9FA;
            border: 1px solid #E0E0E0;
            border-radius: 8px;
            padding: 15px;
            text-align: center;
        }
        .metric-val { font-size: 24px; font-weight: bold; color: #00C896; }
        .metric-lbl { font-size: 14px; color: #666; }
    </style>
    """, unsafe_allow_html=True)

    # 🔥 FIX: Ініціалізація вкладок ПЕРЕД використанням
    tab_list, tab_create, tab_edit = st.tabs(["📋 Список Клієнтів", "➕ Створити Клієнта", "✏️ Редагування"])

    # ========================================================
    # TAB 1: СПИСОК КЛІЄНТІВ (ОГЛЯД)
    # ========================================================
    with tab_list:
        if st.button("🔄 Оновити дані"):
            st.rerun()

        try:
            # 1. Завантажуємо всі проекти
            projects = supabase.table("projects").select("*").order("created_at", desc=True).execute().data
            
            if projects:
                total_clients = len(projects)
                active_clients = len([p for p in projects if p.get('status') == 'active'])
                
                # Виводимо плашки з метриками
                c1, c2, c3 = st.columns(3)
                c1.markdown(f"<div class='metric-box'><div class='metric-val'>{total_clients}</div><div class='metric-lbl'>Всього клієнтів</div></div>", unsafe_allow_html=True)
                c2.markdown(f"<div class='metric-box'><div class='metric-val'>{active_clients}</div><div class='metric-lbl'>Активних (Paid)</div></div>", unsafe_allow_html=True)
                c3.markdown(f"<div class='metric-box'><div class='metric-val'>{total_clients - active_clients}</div><div class='metric-lbl'>Тріал (Trial)</div></div>", unsafe_allow_html=True)
                
                st.write("") 

                client_data = []
                
                with st.spinner("Завантаження статистики по клієнтах..."):
                    for p in projects:
                        pid = p['id']
                        
                        # 🔥 FIX: Перетворення результатів count() у звичайний int(),
                        # щоб уникнути помилки "int64 is not JSON serializable"
                        
                        # А. Кількість ключових слів
                        kw_res = supabase.table("keywords").select("id", count="exact").eq("project_id", pid).execute()
                        # Використовуємо int(), щоб перетворити numpy.int64 або інші типи
                        kw_count = int(kw_res.count) if kw_res.count is not None else 0
                        
                        # Б. Кількість запусків (Scan Runs)
                        scan_res = supabase.table("scan_results").select("id", count="exact").eq("project_id", pid).execute()
                        scan_count = int(scan_res.count) if scan_res.count is not None else 0
                        
                        # В. Офіційні джерела (список)
                        try:
                            assets_res = supabase.table("official_assets").select("domain_or_url").eq("project_id", pid).execute()
                            assets_list = [a['domain_or_url'] for a in assets_res.data]
                            assets_str = ", ".join(assets_list) if assets_list else "-"
                        except:
                            assets_str = "-"

                        # Г. CRON Статус
                        is_cron = p.get("cron_enabled", False)
                        cron_status = "✅ ON" if is_cron else "⏸️ OFF"
                        cron_freq = p.get("cron_frequency", "-") if is_cron else "-"

                        client_data.append({
                            "ID": pid,
                            "Email / User ID": p.get("user_id", "N/A"), # Виводимо ID користувача (Email)
                            "Бренд": p.get("brand_name"),
                            "Домен": p.get("domain"),
                            "Регіон": p.get("region", "UA"),
                            "Статус": p.get("status", "trial").upper(),
                            "CRON": cron_status,
                            "Частота": cron_freq,
                            "Запитів": kw_count,
                            "Аналізів": scan_count,
                            "Джерела": assets_str,
                            "Створено": p.get("created_at")[:10] if p.get("created_at") else "-"
                        })
                
                df = pd.DataFrame(client_data)
                
                # 3. Відображення таблиці
                st.dataframe(
                    df,
                    use_container_width=True,
                    column_config={
                        "ID": st.column_config.TextColumn("ID", help="Скопіюйте це ID для редагування", width="small"),
                        "Email / User ID": st.column_config.TextColumn("Email / User ID", width="medium"),
                        "Статус": st.column_config.TextColumn("Статус", help="Trial або Active", width="small"),
                        "CRON": st.column_config.TextColumn("Auto", width="small"),
                        "Запитів": st.column_config.ProgressColumn("Запитів", format="%d", min_value=0, max_value=max(df["Запитів"].max(), 10)),
                        "Аналізів": st.column_config.NumberColumn("Запусків"),
                        "Джерела": st.column_config.TextColumn("Whitelist", width="medium")
                    },
                    hide_index=True
                )
            else:
                st.info("У базі поки немає проектів.")
                
        except Exception as e:
            st.error(f"Помилка завантаження списку клієнтів: {e}")

    # ========================================================
    # TAB 2: СТВОРИТИ КЛІЄНТА (ONBOARDING FOR ADMIN)
    # ========================================================
    with tab_create:
        st.markdown("##### 👤 Додати нового клієнта")
        st.caption("Створення проекту для користувача вручну.")
        
        with st.form("admin_create_client_form"):
            c1, c2 = st.columns(2)
            with c1:
                new_uid = st.text_input("User Email / UUID", help="Email або ID користувача для входу")
                new_brand = st.text_input("Назва Бренду", placeholder="Напр. Nova Poshta")
            
            with c2:
                new_domain = st.text_input("Домен сайту", placeholder="novaposhta.ua")
                new_region = st.selectbox("Регіон", ["UA", "US", "EU", "Global"])
            
            new_status = st.selectbox("Початковий статус", ["trial", "active"])
            
            st.markdown("**Налаштування:**")
            new_assets = st.text_area("Офіційні джерела (Whitelist)", placeholder="https://instagram.com/...", help="Через кому")
            new_kws = st.text_area("Початкові запити (Ключові слова)", placeholder="доставка, ціна...", help="По одному в рядок")

            submitted_create = st.form_submit_button("✅ Створити Клієнта", type="primary")
            
            if submitted_create:
                if new_uid and new_brand:
                    try:
                        res = supabase.table("projects").insert({
                            "user_id": new_uid,
                            "brand_name": new_brand,
                            "domain": new_domain,
                            "region": new_region,
                            "status": new_status
                        }).execute()
                        
                        if res.data:
                            new_pid = res.data[0]['id']
                            
                            # Джерела
                            if new_assets:
                                asset_list = [a.strip() for a in new_assets.replace("\n", ",").split(",") if a.strip()]
                                if asset_list:
                                    asset_data = [{"project_id": new_pid, "domain_or_url": a, "type": "website"} for a in asset_list]
                                    supabase.table("official_assets").insert(asset_data).execute()
                            
                            # Запити
                            if new_kws:
                                kw_list = [k.strip() for k in new_kws.split("\n") if k.strip()]
                                if kw_list:
                                    kw_data = [{"project_id": new_pid, "keyword_text": k, "is_active": True, "is_cron_active": False} for k in kw_list]
                                    supabase.table("keywords").insert(kw_data).execute()

                            st.success(f"Клієнт {new_brand} успішно створений! ID: {new_pid}")
                            time.sleep(1)
                            st.rerun()
                    except Exception as e:
                        st.error(f"Помилка створення: {e}")
                else:
                    st.warning("Вкажіть User ID та Назву бренду.")

    # ========================================================
    # TAB 3: РЕДАГУВАННЯ (ЗМІНА СТАТУСУ, КРОН)
    # ========================================================
    with tab_edit:
        st.markdown("##### ✏️ Керування існуючим проектом")
        
        try:
            all_projs = supabase.table("projects").select("id, brand_name, user_id").execute().data
            # Формат: "Brand (Email)"
            proj_options = {f"{p['brand_name']} ({p.get('user_id')})": p['id'] for p in all_projs}
            
            selected_label = st.selectbox("Оберіть клієнта для редагування:", list(proj_options.keys()), index=None)
            
            if selected_label:
                pid = proj_options[selected_label]
                
                # Завантажуємо дані
                curr_data = supabase.table("projects").select("*").eq("id", pid).single().execute().data
                
                st.divider()
                
                with st.form("edit_client_form"):
                    st.subheader("1. Загальні налаштування")
                    c1, c2 = st.columns(2)
                    with c1:
                        edit_brand = st.text_input("Назва бренду", value=curr_data.get("brand_name"))
                        
                        # Статус (включаючи blocked)
                        status_opts = ["trial", "active", "expired", "blocked"]
                        curr_status = curr_data.get("status", "trial")
                        st_idx = status_opts.index(curr_status) if curr_status in status_opts else 0
                        
                        edit_status = st.selectbox("Статус (План)", status_opts, index=st_idx)
                    
                    with c2:
                        region_opts = ["UA", "US", "EU", "Global"]
                        curr_reg = curr_data.get("region", "UA")
                        reg_idx = region_opts.index(curr_reg) if curr_reg in region_opts else 0
                        
                        edit_region = st.selectbox("Регіон", region_opts, index=reg_idx)
                        
                        st.multiselect("Активні моделі (Доступ)", ["Perplexity", "GPT-4o", "Gemini"], default=["Perplexity", "GPT-4o", "Gemini"], disabled=True)

                    # БЛОК CRON
                    st.divider()
                    st.subheader("2. Автоматизація (CRON)")
                    
                    cc1, cc2 = st.columns(2)
                    with cc1:
                        edit_cron_enabled = st.checkbox("✅ Увімкнути авто-сканування", value=curr_data.get("cron_enabled", False))
                    
                    with cc2:
                        freq_opts = ["daily", "weekly", "monthly"]
                        curr_freq = curr_data.get("cron_frequency", "daily")
                        freq_idx = freq_opts.index(curr_freq) if curr_freq in freq_opts else 0
                        
                        edit_cron_freq = st.selectbox("Частота запуску", freq_opts, index=freq_idx)

                    st.markdown("---")
                    st.caption(f"Project ID: {pid} | Created: {curr_data.get('created_at')}")

                    submitted_edit = st.form_submit_button("💾 Зберегти зміни", type="primary")
                    
                    if submitted_edit:
                        try:
                            supabase.table("projects").update({
                                "brand_name": edit_brand,
                                "status": edit_status,
                                "region": edit_region,
                                "cron_enabled": edit_cron_enabled,
                                "cron_frequency": edit_cron_freq
                            }).eq("id", pid).execute()
                            
                            st.success("Налаштування оновлено!")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Помилка оновлення: {e}")

        except Exception as e:
            st.error(f"Помилка завантаження списку редагування: {e}")


def main():
    # 1. Session Check
    check_session()

    # 2. If not logged in -> Show Auth Page
    if not st.session_state.get("user"):
        show_auth_page()  # <--- CHANGED THIS LINE
        return

    # 3. ОТРИМАННЯ ДАНИХ ПРОЕКТУ
    # Якщо користувач залогінений, але проект ще не завантажено в сесію - пробуємо знайти
    if not st.session_state.get("current_project"):
        try:
            user_id = st.session_state["user"].id
            # Шукаємо проект користувача
            resp = supabase.table("projects").select("*").eq("user_id", user_id).execute()
            if resp.data:
                # Якщо знайшли - записуємо в сесію (беремо перший)
                st.session_state["current_project"] = resp.data[0]
                st.rerun() # Перезавантажуємо, щоб оновити інтерфейс
        except Exception:
            pass

    # 4. ЛОГІКА ONBOARDING
    # Якщо проекту все ще немає (і це не адмін, бо адмін може не мати свого проекту)
    if st.session_state.get("current_project") is None and st.session_state.get("role") != "admin":
        # Показуємо кнопку виходу в сайдбарі (щоб не застряг)
        with st.sidebar:
            st.image("https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png", width=150) # Або текст
            if st.button("Вийти"):
                logout()
        
        # Запускаємо Майстер створення
        onboarding_wizard()
    
    # 5. ОСНОВНИЙ ДОДАТОК
    else:
        # Меню
        page = sidebar_menu()

        if page == "Дашборд":
            show_dashboard()
        elif page == "Перелік запитів":
            show_keywords_page()
        elif page == "Джерела":
            show_sources_page()
        elif page == "Конкуренти":
            show_competitors_page()
        elif page == "Рекомендації":
            show_recommendations_page()
        elif page == "GPT-Visibility":
            show_chat_page()
        elif page == "Адмін":
            if st.session_state.get("role") == "admin":
                show_admin_page()
            else:
                st.error("Доступ заборонено.")

if __name__ == "__main__":
    main()
