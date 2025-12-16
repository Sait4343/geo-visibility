import time
from datetime import datetime, timedelta, date, time as dt_time # Додано dt_time
import plotly.express as px 
import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
import extra_streamlit_components as stx
from streamlit_option_menu import option_menu
from supabase import create_client, Client
import numpy as np # Потрібно для адмінки
import json


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
N8N_RECO_URL = "https://virshi.app.n8n.cloud/webhook/recommendations"  
N8N_CHAT_WEBHOOK = "https://virshi.app.n8n.cloud/webhook/webhook/chat-bot" 


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
    ОНОВЛЕНО: Додано header авторизації (virshi-auth), щоб виправити помилку 403.
    """
    try:
        payload = {
            "brand": brand,
            "domain": domain,
            "industry": industry,
            "products": products,
        }

        # 🔥 ВАЖЛИВО: Додаємо заголовок авторизації
        headers = {
            "virshi-auth": "hi@virshi.ai2025"
        }

        # Передаємо headers у запит
        response = requests.post(N8N_GEN_URL, json=payload, headers=headers, timeout=20)

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
    ВЕРСІЯ: RESTORED ORIGINAL PAYLOAD + TRIAL LIMIT.
    1. Формат даних повернуто до робочого стану (без brand_name_lower, без чистки URL).
    2. Працює блокування повторних запусків для Trial.
    """
    import requests
    import streamlit as st
    
    # --- 1. ПІДКЛЮЧЕННЯ ДО БД ---
    if 'supabase' in st.session_state:
        supabase = st.session_state['supabase']
    elif 'supabase' in globals():
        supabase = globals()['supabase']
    else:
        st.error("🚨 Помилка: Немає підключення до БД.")
        return False

    MODEL_MAPPING = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }

    # 2. ОТРИМАННЯ СТАТУСУ
    current_proj = st.session_state.get("current_project")
    
    status = "trial"
    if current_proj and current_proj.get("id") == project_id:
        status = current_proj.get("status", "trial")
    
    if status == "blocked":
        st.error("⛔ Проект заблоковано (BLOCKED). Зверніться до адміністратора.")
        return False

    if not models:
        models = ["Perplexity"]

    # ==========================================
    # 🔥 ЛОГІКА ТРІАЛУ (ЗАХИСТ)
    # ==========================================
    if status == "trial":
        is_only_gemini = True
        for m in models:
            if "Gemini" not in m and "gemini" not in m:
                is_only_gemini = False
                break
        
        if not is_only_gemini:
            st.error("⛔ У статусі TRIAL ручний запуск обмежено. Доступно лише через Google Gemini.")
            return False

        # Перевірка на повторний запуск (чи є записи в scan_results)
        try:
            existing = supabase.table("scan_results")\
                .select("id", count="exact")\
                .eq("project_id", project_id)\
                .limit(1)\
                .execute()
            
            # Якщо count > 0 -> вже сканували -> Блокуємо
            if existing.data or (existing.count and existing.count > 0):
                st.error("⛔ Аналіз неможливий у статусі TRIAL (ліміт вичерпано). Будь ласка, зверніться в техпідтримку на пошту hi@virshi.ai, щоб отримати статус ACTIVE.")
                return False
        except Exception as e:
            # Не блокуємо при помилці запиту, щоб не ламати логіку, але виводимо в консоль
            print(f"Trial check error: {e}")

    # ==========================================
    # 🚀 ВІДПРАВКА (РОБОЧИЙ ВАРІАНТ)
    # ==========================================
    try:
        user = st.session_state.get("user")
        user_email = user.email if user else "no-reply@virshi.ai"
        
        if isinstance(keywords, str):
            keywords = [keywords]

        success_count = 0

        # 3. ОТРИМУЄМО ОФІЦІЙНІ ДЖЕРЕЛА (Без змін, як у робочому варіанті)
        official_assets = []
        try:
            assets_resp = supabase.table("official_assets")\
                .select("domain_or_url")\
                .eq("project_id", project_id)\
                .execute()
            # Беремо як є, без .lower() і без replace(), бо n8n очікує оригінал
            official_assets = [item["domain_or_url"] for item in assets_resp.data] if assets_resp.data else []
        except Exception as e:
            print(f"Error fetching assets: {e}")
            official_assets = []

        headers = {"virshi-auth": "hi@virshi.ai2025"}

        # 4. ЦИКЛ ВІДПРАВКИ
        for ui_model_name in models:
            tech_model_id = MODEL_MAPPING.get(ui_model_name, ui_model_name)

            # Формуємо JSON точнісінько як у вашому прикладі "working JSON"
            payload = {
                "project_id": project_id,
                "keywords": keywords, 
                "brand_name": brand_name,
                # "brand_name_lower" ПРИБРАНО - це ламало n8n
                "user_email": user_email,
                "provider": tech_model_id,
                "models": [tech_model_id],
                "official_assets": official_assets 
            }
            
            try:
                # Переконайтеся, що змінна N8N_ANALYZE_URL доступна глобально
                response = requests.post(
                    N8N_ANALYZE_URL, 
                    json=payload, 
                    headers=headers, 
                    timeout=20
                )
                
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


def trigger_ai_recommendation(user, project, category, context_text):
    """
    Відправляє запит на AI для генерації HTML-звіту.
    """
    import requests
    from datetime import datetime
    
    headers = {
        "virshi-auth": "hi@virshi.ai2025"
    }
    
    # Формуємо повний payload
    payload = {
        "timestamp": datetime.now().isoformat(),
        # Інформація про користувача
        "user_id": user.id if user else "unknown",
        "user_email": user.email if user else "unknown",
        
        # Інформація про проект
        "project_id": project.get("id"),
        "brand_name": project.get("brand_name"),
        "domain": project.get("domain"),
        
        # Деталі запиту
        "category": category, 
        "request_context": context_text,
        "request_type": "html_report"
    }
    
    try:
        response = requests.post(N8N_RECO_URL, json=payload, headers=headers, timeout=120)
        
        if response.status_code == 200:
            # Спробуємо розпарсити JSON, якщо n8n повертає об'єкт
            try:
                data = response.json()
                # Шукаємо HTML у різних полях
                return data.get("html") or data.get("output") or data.get("report") or str(data)
            except:
                # Якщо повернувся просто текст/html
                return response.text
        else:
            return f"<p style='color:red; font-weight:bold;'>Error from AI Provider: {response.status_code}</p>"
            
    except Exception as e:
        return f"<p style='color:red; font-weight:bold;'>Connection Error: {e}</p>"


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
    Майстер створення проекту.
    ВЕРСІЯ: REGION SELECT + TIMEOUT FIX.
    1. Регіон: Випадаючий список (Ukraine, USA, Europe, Global).
    2. База даних: Регіон записується динамічно.
    """
    import requests
    import time
    
    # Ініціалізація змінних сесії
    if "onboarding_stage" not in st.session_state:
        st.session_state["onboarding_stage"] = 2
        st.session_state["generated_prompts"] = []
    
    # CSS для зелених номерів
    st.markdown("""
    <style>
        .green-number-small {
            background-color: #00C896;
            color: white;
            width: 24px;
            height: 24px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: bold;
            font-size: 12px;
            margin-top: 8px;
        }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown("## 🚀 Налаштування Проекту")
    step = st.session_state.get("onboarding_step", 2) 

    with st.container(border=True):
        # ========================================================
        # КРОК 1: ВВІД ДАНИХ
        # ========================================================
        if step == 2:
            st.subheader("Крок 1: Введіть дані про ваш бренд")
            c1, c2 = st.columns(2)
            with c1:
                brand = st.text_input("Назва бренду", placeholder="Monobank", value=st.session_state.get("temp_brand", ""))
                industry = st.text_input("Галузь бренду / ніша", placeholder="Фінтех", value=st.session_state.get("temp_industry", ""))
            with c2:
                domain = st.text_input("Домен (офіційний сайт)", placeholder="monobank.ua", value=st.session_state.get("temp_domain", ""))
                
                # 🔥 UPDATE: Випадаючий список замість фіксованого тексту
                region_options = ["Ukraine", "USA", "Europe", "Global"]
                # Якщо раніше вже обирали, беремо збережене, інакше дефолт Ukraine (0 індекс)
                saved_region = st.session_state.get("temp_region", "Ukraine")
                try:
                    idx = region_options.index(saved_region)
                except:
                    idx = 0
                
                region = st.selectbox("Регіон", options=region_options, index=idx)

            products = st.text_area("Продукти / Послуги", value=st.session_state.get("temp_products", ""))

            if st.button("Згенерувати запити"):
                if brand and domain and industry and products:
                    st.session_state["temp_brand"] = brand
                    st.session_state["temp_domain"] = domain
                    st.session_state["temp_industry"] = industry
                    st.session_state["temp_products"] = products
                    # 🔥 UPDATE: Зберігаємо обраний регіон
                    st.session_state["temp_region"] = region

                    with st.spinner("Генеруємо релевантні запити..."):
                        # Виклик оновленої функції з таймаутом 60с
                        prompts = n8n_generate_prompts(brand, domain, industry, products)
                        if prompts:
                            st.session_state["generated_prompts"] = prompts
                            st.session_state["onboarding_step"] = 3
                            st.rerun()
                        # Якщо помилка таймауту, повідомлення виведе сама функція n8n_generate_prompts
                else:
                    st.warning("⚠️ Будь ласка, заповніть всі поля.")

        # ========================================================
        # КРОК 2: ПЕРЕВІРКА ТА ЗАПУСК
        # ========================================================
        elif step == 3:
            st.subheader("Крок 2: Перевірка та запуск")
            
            prompts_list = st.session_state.get("generated_prompts", [])
            
            if not prompts_list:
                st.warning("Список запитів порожній.")
                if st.button("Назад"):
                    st.session_state["onboarding_step"] = 2
                    st.rerun()
                return

            st.markdown("Перевірте та відредагуйте запити перед запуском:")
            st.write("") 

            selected_indices = []

            # --- ЦИКЛ ВИВОДУ КАРТОК З РЕДАГУВАННЯМ ---
            for i, kw in enumerate(prompts_list):
                edit_key = f"edit_mode_row_{i}"
                if edit_key not in st.session_state:
                    st.session_state[edit_key] = False

                with st.container(border=True):
                    c_chk, c_num, c_text, c_btn = st.columns([0.5, 0.5, 8, 1])
                    
                    # 1. Чекбокс
                    with c_chk:
                        st.write("") 
                        if st.checkbox("", value=True, key=f"chk_final_{i}"):
                            selected_indices.append(i)
                    
                    # 2. Номер
                    with c_num:
                        st.markdown(f"<div class='green-number-small'>{i+1}</div>", unsafe_allow_html=True)

                    # 3. Текст / Поле
                    with c_text:
                        if st.session_state[edit_key]:
                            new_val = st.text_input("", value=kw, key=f"input_kw_{i}", label_visibility="collapsed")
                        else:
                            st.markdown(f"<div style='padding-top: 8px; font-size: 15px;'>{kw}</div>", unsafe_allow_html=True)

                    # 4. Кнопка
                    with c_btn:
                        st.write("") 
                        if st.session_state[edit_key]:
                            if st.button("💾", key=f"save_kw_{i}", help="Зберегти"):
                                st.session_state["generated_prompts"][i] = new_val
                                st.session_state[edit_key] = False
                                st.rerun()
                        else:
                            if st.button("✏️", key=f"edit_kw_{i}", help="Редагувати"):
                                st.session_state[edit_key] = True
                                st.rerun()

            final_kws_to_send = [st.session_state["generated_prompts"][idx] for idx in selected_indices]
            
            st.divider()
            c_info, c_launch = st.columns([2, 1])
            with c_info:
                st.markdown(f"**Готово до запуску:** {len(final_kws_to_send)} запитів")
            
            with c_launch:
                if st.button("🚀 Зберегти та Запустити аналіз", type="primary", use_container_width=True):
                    if final_kws_to_send:
                        try:
                            # 1. ЗБИРАЄМО ДАНІ
                            user_id = st.session_state["user"].id
                            brand_name = st.session_state.get("temp_brand")
                            domain_name = st.session_state.get("temp_domain")
                            # 🔥 UPDATE: Беремо регіон зі стейту
                            region_val = st.session_state.get("temp_region", "Ukraine")
                            
                            # 2. СТВОРЮЄМО ПРОЕКТ
                            res = supabase.table("projects").insert({
                                "user_id": user_id, 
                                "brand_name": brand_name,
                                "domain": domain_name,
                                "region": region_val,  # <--- Записуємо обраний регіон
                                "status": "trial"
                            }).execute()

                            if res.data:
                                proj_data = res.data[0]
                                proj_id = proj_data["id"]
                                
                                st.session_state["current_project"] = proj_data

                                # 3. ЗАПИСУЄМО WHITELIST
                                clean_d = domain_name.replace("https://", "").replace("http://", "").strip().rstrip("/")
                                try:
                                    supabase.table("official_assets").insert({
                                        "project_id": proj_id, 
                                        "domain_or_url": clean_d,
                                        "type": "website"
                                    }).execute()
                                except Exception:
                                    pass 

                                # 4. ЗАПИСУЄМО KEYWORDS
                                kws_data = [{"project_id": proj_id, "keyword_text": kw, "is_active": True} for kw in final_kws_to_send]
                                supabase.table("keywords").insert(kws_data).execute()
                                
                                # 5. ВІДПРАВКА НА N8N
                                my_bar = st.progress(0, text="Ініціалізація AI-аналітика...")
                                total_kws = len(final_kws_to_send)

                                for i, single_kw in enumerate(final_kws_to_send):
                                    progress_pct = (i + 1) / total_kws
                                    my_bar.progress(progress_pct, text=f"Аналіз запиту: {single_kw}...")
                                    
                                    n8n_trigger_analysis(
                                        project_id=proj_id, 
                                        keywords=[single_kw],     
                                        brand_name=brand_name,
                                        models=["Google Gemini"]  
                                    )
                                    time.sleep(0.5) 

                                my_bar.progress(1.0, text="✅ Проект створено успішно!")
                                time.sleep(1)

                                # Фінал
                                st.session_state["onboarding_step"] = 2 
                                st.success("Успіх!")
                                st.rerun()

                        except Exception as e:
                            st.error(f"Помилка створення проекту: {e}")
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

def show_recommendations_page():
    """
    Сторінка рекомендацій.
    ВЕРСІЯ: RENAMED FILES & BUTTONS.
    File prefix: "Recommendations_"
    Button label: "Завантажити Рекомендації"
    """
    import streamlit as st
    import pandas as pd
    import streamlit.components.v1 as components
    from datetime import datetime, timedelta

    # --- 1. ПІДКЛЮЧЕННЯ ---
    if 'supabase' in st.session_state:
        supabase = st.session_state['supabase']
    elif 'supabase' in globals():
        supabase = globals()['supabase']
    else:
        st.error("🚨 DB Error: Немає з'єднання з базою даних.")
        return

    proj = st.session_state.get("current_project")
    user = st.session_state.get("user")
    
    if not proj:
        st.info("Спочатку оберіть проект.")
        return

    st.title(f"💡 Центр рекомендацій: {proj.get('brand_name')}")

    # --- 2. КАТЕГОРІЇ ---
    CATEGORIES = {
        "Digital": {
            "title": "Digital & Technical GEO",
            "desc": "Технічна оптимізація екосистеми бренду для алгоритмів AI.",
            "value": "LLM (ChatGPT, Gemini) — це програми. Якщо сайт технічно складний для них, вони його ігнорують. Ми аналізуємо код, розмітку Schema.org та доступність для ботів.",
            "prompt_context": "Analyze technical SEO, Schema markup, site structure, and data accessibility for LLM crawling. Focus on Technical GEO factors."
        },
        "Content": {
            "title": "Content Strategy",
            "desc": "Створення контенту, який AI захоче цитувати.",
            "value": "AI любить факти і структуру. Ми дамо план: які статті писати і як їх оформлювати, щоб стати 'джерелом істини' для нейромереж.",
            "prompt_context": "Generate content strategy optimized for Generative Search. Focus on answer structure, NLP-friendly formats, and topical authority."
        },
        "PR": {
            "title": "PR & Brand Authority",
            "desc": "Побудова авторитету через зовнішні джерела.",
            "value": "AI довіряє тому, про що пишуть авторитетні медіа. Ми визначимо, де вам треба з'явитися (Wiki, ЗМІ), щоб алгоритми вважали вас лідером.",
            "prompt_context": "Analyze brand authority signals, mentions in tier-1 media, and external trust factors influencing LLM perception."
        },
        "Social": {
            "title": "Social Media & UGC",
            "desc": "Вплив соціальних сигналів на видачу.",
            "value": "Gemini та Perplexity читають Reddit, LinkedIn та X у реальному часі. Ми покажемо, як керувати дискусією там, щоб AI бачив позитив.",
            "prompt_context": "Analyze social signals, User Generated Content (Reddit, LinkedIn, Reviews), and their impact on real-time AI answers."
        }
    }

    main_tab, history_tab = st.tabs(["🚀 Замовити рекомендацію", "📚 Історія рекомендацій"])

    # Підготовка безпечної назви бренду для файлів
    safe_brand_name = proj.get('brand_name', 'Brand').replace(" ", "_")

    # ========================================================
    # TAB 1: ЗАМОВЛЕННЯ
    # ========================================================
    with main_tab:
        st.markdown("Оберіть напрямок, щоб отримати стратегію **Generative Engine Optimization**.")
        
        cat_names = list(CATEGORIES.keys())
        cat_tabs = st.tabs([CATEGORIES[c]["title"] for c in cat_names])

        for idx, cat_key in enumerate(cat_names):
            info = CATEGORIES[cat_key]
            with cat_tabs[idx]:
                with st.container(border=True):
                    st.subheader(info["title"])
                    st.markdown(f"**Що це:** {info['desc']}")
                    st.info(f"💎 **Навіщо це вам:**\n\n{info['value']}")
                    st.write("") 
                    
                    # Кнопка генерації
                    btn_label = f"✨ Отримати рекомендації ({info['title']})"
                    
                    if st.button(btn_label, key=f"btn_rec_{cat_key}", type="primary", use_container_width=True):
                        
                        if proj.get('status') == 'blocked':
                            st.error("Проект заблоковано.")
                        else:
                            st.warning("⏳ Розпочато формування рекомендацій. Будь ласка, не закривайте сторінку і дочекайтеся завершення (це може зайняти до 60 секунд).")
                            
                            with st.spinner("Аналіз даних та генерація звіту..."):
                                if 'trigger_ai_recommendation' in globals():
                                    html_res = trigger_ai_recommendation(
                                        user=user, project=proj, category=info["title"], context_text=info["prompt_context"]
                                    )
                                    try:
                                        supabase.table("strategy_reports").insert({
                                            "project_id": proj["id"], 
                                            "category": cat_key, 
                                            "html_content": html_res, 
                                            "created_at": datetime.now().isoformat()
                                        }).execute()
                                        
                                        st.success("✅ Рекомендації успішно сформовано!")
                                        st.markdown(f"""
                                            <div style="padding:15px; border:1px solid #00C896; border-radius:5px; background-color:#f0fff4;">
                                                <p>Ваш звіт збережено. Перейдіть у вкладку <b>"Історія рекомендацій"</b>, щоб переглянути його.</p>
                                            </div>
                                        """, unsafe_allow_html=True)
                                        
                                    except Exception as e:
                                        st.error(f"Помилка збереження в БД: {e}")
                                        with st.expander("Резервний перегляд", expanded=True):
                                            components.html(html_res, height=600, scrolling=True)
                                            # Кнопка скачування (Резервна)
                                            st.download_button(
                                                "📥 Завантажити Рекомендації", 
                                                html_res, 
                                                file_name=f"Recommendations_{cat_key}_{safe_brand_name}.html", 
                                                mime="text/html"
                                            )
                                else:
                                    st.error("Функція trigger_ai_recommendation не знайдена.")

    # ========================================================
    # TAB 2: ІСТОРІЯ
    # ========================================================
    with history_tab:
        c_h1, c_h2 = st.columns(2)
        with c_h1:
            sel_cat_hist = st.multiselect("Фільтр по категорії", list(CATEGORIES.keys()), default=[])
        with c_h2:
            date_filter_options = ["Весь час", "Сьогодні", "Останні 7 днів", "Останні 30 днів"]
            sel_date_range = st.selectbox("Період", date_filter_options)

        try:
            query = supabase.table("strategy_reports").select("*").eq("project_id", proj["id"]).order("created_at", desc=True)
            hist_resp = query.execute()
            reports = hist_resp.data if hist_resp.data else []
            
            if reports:
                df_rep = pd.DataFrame(reports)
                df_rep['created_at_dt'] = pd.to_datetime(df_rep['created_at'])
                
                # Фільтри
                if sel_cat_hist:
                    df_rep = df_rep[df_rep['category'].isin(sel_cat_hist)]
                
                now = datetime.now(df_rep['created_at_dt'].dt.tz)
                
                if sel_date_range == "Сьогодні":
                    df_rep = df_rep[df_rep['created_at_dt'].dt.date == now.date()]
                elif sel_date_range == "Останні 7 днів":
                    df_rep = df_rep[df_rep['created_at_dt'] >= (now - timedelta(days=7))]
                elif sel_date_range == "Останні 30 днів":
                    df_rep = df_rep[df_rep['created_at_dt'] >= (now - timedelta(days=30))]
                
                if df_rep.empty:
                    st.info("За обраними критеріями звітів не знайдено.")
                else:
                    for _, row in df_rep.iterrows():
                        cat_nice = CATEGORIES.get(row['category'], {}).get('title', row['category'])
                        try: date_str = row['created_at'][:16].replace('T', ' ')
                        except: date_str = "-"
                        
                        # Формуємо красиву дату для файлу (наприклад: 2023-10-25_14-30)
                        date_file = date_str.replace(" ", "_").replace(":", "-")

                        with st.expander(f"📑 {cat_nice} | {date_str}"):
                            c_dl, c_del = st.columns([4, 1])
                            
                            with c_dl:
                                # 🔥 Нова назва файлу: Recommendations_Category_Brand_Date.html
                                file_n = f"Recommendations_{row['category']}_{safe_brand_name}_{date_file}.html"
                                
                                # 🔥 Нова назва кнопки (без .html)
                                st.download_button(
                                    label="📥 Завантажити Рекомендації", 
                                    data=row['html_content'], 
                                    file_name=file_n, 
                                    mime="text/html",
                                    key=f"dl_hist_{row['id']}"
                                )
                            
                            with c_del:
                                del_key = f"confirm_del_{row['id']}"
                                if del_key not in st.session_state:
                                    st.session_state[del_key] = False

                                if not st.session_state[del_key]:
                                    if st.button("🗑️", key=f"pre_del_{row['id']}", help="Видалити звіт"):
                                        st.session_state[del_key] = True
                                        st.rerun()
                                else:
                                    col_yes, col_no = st.columns(2)
                                    if col_yes.button("✅", key=f"yes_{row['id']}"):
                                        supabase.table("strategy_reports").delete().eq("id", row['id']).execute()
                                        st.session_state[del_key] = False
                                        st.rerun()
                                    if col_no.button("❌", key=f"no_{row['id']}"):
                                        st.session_state[del_key] = False
                                        st.rerun()
                            
                            st.divider()
                            components.html(row['html_content'], height=500, scrolling=True)
            else:
                st.info("Історія рекомендацій порожня. Згенеруйте першу стратегію.")
                
        except Exception as e:
            st.warning(f"Неможливо завантажити історію: {e}")

def show_faq_page():
    """
    Сторінка FAQ & Support.
    ВЕРСІЯ: TOP-20 QUESTIONS + CONTACTS.
    """
    import streamlit as st

    st.title("❓ Центр підтримки та FAQ")

    # --- 1. Цінність платформи ---
    with st.container(border=True):
        st.markdown("### 🚀 Про Virshi.ai Visibility Platform")
        st.markdown("""
        **Virshi.ai** — це інструмент нового покоління для **GEO (Generative Engine Optimization)**. 
        Ми допомагаємо брендам розуміти, як саме штучний інтелект (ChatGPT, Perplexity, Gemini) бачить ваш бізнес, 
        і надаємо інструменти для покращення вашої видимості у відповідях AI.
        
        **Наша цінність:**
        * 🔍 **Прозорість:** Бачте те, що бачить AI.
        * 📊 **Вимірюваність:** Перетворіть абстрактні "згадки" на конкретні метрики (SOV, Rank, Sentiment).
        * 📈 **Вплив:** Отримуйте рекомендації, як потрапити у відповіді AI та рекомендації.
        """)
        st.info("📧 **Технічна підтримка:** support@virshi.ai")

    st.divider()
    st.subheader("Топ-20 найчастіших запитань")

    faq_data = [
        ("Що таке GEO (Generative Engine Optimization)?", "Це процес оптимізації контенту вашого бренду, щоб він частіше і якісніше з'являвся у відповідях генеративних моделей (LLM), таких як GPT-4, Gemini тощо."),
        ("Що таке Share of Voice (SOV)?", "Це метрика, яка показує частку згадок вашого бренду серед усіх брендів, знайдених у відповіді на конкретний запит."),
        ("Як визначається тональність?", "Наші алгоритми аналізують контекст згадки (прикметники, емоційне забарвлення) і класифікують її як Позитивну, Нейтральну або Негативну."),
        ("Що таке 'Офіційні джерела' (Whitelist)?", "Це список ваших підконтрольних доменів (сайт, соцмережі). Ми відстежуємо, чи посилається AI саме на них як на джерело істини."),
        ("Як часто оновлюються дані?", "Якщо увімкнено автосканування, дані оновлюються щодня або щотижня (залежно від налаштувань). Ручний запуск дає миттєвий результат."),
        ("Чому мій бренд не знайдено?", "Можливо, AI ще не проіндексував ваш контент, або ваш бренд має низьку авторитетність у темі запиту. Скористайтеся вкладкою 'Рекомендації'."),
        ("Чи можу я додати конкурентів?", "Так, система автоматично визначає конкурентів у відповідях. Ви також можете бачити їх у розділі 'Конкуренти'."),
        ("Які моделі (LLM) підтримуються?", "Наразі ми підтримуємо Perplexity, OpenAI GPT-4o та Google Gemini Pro."),
        ("Чим відрізняється Trial від Active?", "У Trial режимі ви можете сканувати лише обмежену кількість запитів і тільки через Gemini. Active знімає ці обмеження."),
        ("Як працює імпорт запитів?", "Ви можете завантажити Excel-файл або вставити посилання на Google Sheet. Перша колонка має називатися 'Keyword'."),
        ("Що робити, якщо статус проекту 'Blocked'?", "Зверніться до адміністратора або на пошту підтримки для вирішення питань з оплатою або доступом."),
        ("Чи впливає SEO сайту на GEO?", "Так, технічне SEO та якість контенту є фундаментом для того, щоб LLM взагалі могли 'прочитати' ваш сайт."),
        ("Як покращити позицію (Rank)?", "Структуруйте дані, використовуйте списки, чіткі відповіді на питання та збільшуйте кількість цитувань у авторитетних джерелах."),
        ("Чи можна експортувати звіти?", "Так, ви можете завантажувати дані у форматі Excel або генерувати HTML-звіти у вкладці 'Рекомендації'."),
        ("Що таке 'Присутність у запитах'?", "Це відсоток запитів, на які AI хоча б раз згадав ваш бренд (незалежно від позиції)."),
        ("Як змінити назву бренду?", "Назва бренду задається при створенні проекту. Для зміни зверніться до адміністратора."),
        ("Чи бачать інші користувачі мої дані?", "Ні, дані суворо розділені між проектами та користувачами."),
        ("Скільки запитів я можу додати?", "Ліміт залежить від вашого тарифного плану. У Trial версії ліміт зазвичай 10 запитів."),
        ("Що означає помилка 'Timeout'?", "Це означає, що LLM відповідала занадто довго. Спробуйте повторити запит пізніше."),
        ("Як видалити проект?", "Видалення проекту доступне через адміністратора. Надішліть запит на support@virshi.ai.")
    ]

    for question, answer in faq_data:
        with st.expander(f"🔹 {question}"):
            st.write(answer)


def show_reports_page():
    """Сторінка Звітів"""
    st.title("📊 Звіти")
    st.info("Розділ знаходиться в розробці. Тут ви зможете генерувати PDF-звіти за обраний період.")
    
    c1, c2 = st.columns(2)
    with c1:
        st.date_input("Початок періоду")
    with c2:
        st.date_input("Кінець періоду")
        
    st.button("Згенерувати PDF (Demo)", disabled=True)



def show_dashboard():
    """
    Сторінка Дашборд.
    ВЕРСІЯ: FULL UI + CYRILLIC FIX.
    Збережено весь ваш дизайн і вкладки.
    Виправлено: пошук бренду (кирилиця), таймзони.
    """
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    import streamlit as st
    from datetime import datetime, timedelta, timezone # <--- Fix Timezone
    import re # <--- Fix Cyrillic

    # --- 1. ПІДКЛЮЧЕННЯ ---
    if 'supabase' in st.session_state:
        supabase = st.session_state['supabase']
    elif 'supabase' in globals():
        supabase = globals()['supabase']
    else:
        st.error("🚨 Помилка: Змінна 'supabase' не знайдена. Оновіть сторінку.")
        return

    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект.")
        return

    # --- CSS ---
    st.markdown("""
    <style>
        h3 { font-size: 1.15rem !important; font-weight: 600 !important; padding-top: 20px !important; }
        .green-number { background-color: #00C896; color: white; width: 24px; height: 24px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: bold; font-size: 12px; }
        .comp-tag { background: #f3f4f6; padding: 2px 6px; border-radius: 4px; font-size: 11px; color: #555; }
        
        /* Стиль для блоку тональності в картках */
        .sent-container {
            display: flex;
            flex-direction: column;
            align-items: center; 
            margin-top: 10px;
        }
        .sent-header {
            font-size: 12px;
            color: #555;
            margin-bottom: 4px;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 4px;
        }
        .sent-box { 
            display: flex; 
            gap: 12px; 
            font-size: 13px; 
            font-weight: 500; 
            background: #f9fafb;
            padding: 6px 12px;
            border-radius: 6px;
        }
        .sent-item { display: flex; align-items: center; gap: 4px; }
        
        /* Стиль для конкурента в деталізації (Червоний і Великий) */
        .competitor-highlight {
            color: #FF4B4B; 
            font-size: 14px; 
            font-weight: 700;
        }
    </style>
    """, unsafe_allow_html=True)

    st.title(f"📊 Дашборд: {proj.get('brand_name')}")

    # ==============================================================================
    # 2. ОТРИМАННЯ ДАНИХ
    # ==============================================================================
    with st.spinner("Аналіз даних..."):
        try:
            kw_resp = supabase.table("keywords").select("id, keyword_text").eq("project_id", proj["id"]).execute()
            keywords_df = pd.DataFrame(kw_resp.data) if kw_resp.data else pd.DataFrame()
            kw_map = {k['id']: k['keyword_text'] for k in kw_resp.data} if kw_resp.data else {}
            
            scan_resp = supabase.table("scan_results")\
                .select("id, provider, created_at, keyword_id")\
                .eq("project_id", proj["id"])\
                .order("created_at", desc=True)\
                .limit(1000)\
                .execute() # <--- Ліміт збільшено для точності
            scans_df = pd.DataFrame(scan_resp.data) if scan_resp.data else pd.DataFrame()
            
            mentions_df = pd.DataFrame()
            sources_df = pd.DataFrame()
            
            if not scans_df.empty:
                scan_ids = scans_df['id'].tolist()
                m_resp = supabase.table("brand_mentions").select("*").in_("scan_result_id", scan_ids).execute()
                if m_resp.data: mentions_df = pd.DataFrame(m_resp.data)
                
                s_resp = supabase.table("extracted_sources").select("*").in_("scan_result_id", scan_ids).execute()
                if s_resp.data: sources_df = pd.DataFrame(s_resp.data)

        except Exception as e:
            st.error(f"Помилка завантаження даних: {e}")
            return

    if scans_df.empty:
        st.info("Даних ще немає. Запустіть сканування.")
        return

    # ==============================================================================
    # 3. ОБРОБКА ДАНИХ (ТУТ ВИПРАВЛЕННЯ!)
    # ==============================================================================
    def norm_provider(p):
        p = str(p).lower()
        if 'perplexity' in p: return 'Perplexity'
        if 'gpt' in p: return 'OpenAI GPT'
        if 'gemini' in p: return 'Google Gemini'
        return 'Other'

    scans_df['provider_ui'] = scans_df['provider'].apply(norm_provider)
    scans_df['created_at'] = pd.to_datetime(scans_df['created_at'])

    # 🔥 FIX 1: Нормалізація для Кирилиці
    def normalize_brand_name(name):
        if not name: return ""
        s = str(name).lower()
        return "".join(c for c in s if c.isalnum())

    target_brand_raw = proj.get('brand_name', '').strip()
    target_clean = normalize_brand_name(target_brand_raw)
    
    if not mentions_df.empty:
        mentions_df['mention_count'] = pd.to_numeric(mentions_df['mention_count'], errors='coerce').fillna(0)
        mentions_df['rank_position'] = pd.to_numeric(mentions_df['rank_position'], errors='coerce').fillna(0)
        
        df_full = pd.merge(mentions_df, scans_df, left_on='scan_result_id', right_on='id', suffixes=('_m', '_s'))
        
        # 🔥 FIX 2: Розумна перевірка "Це мій бренд?"
        def check_is_target(row):
            if row.get('is_my_brand') is True: return True
            
            row_brand = normalize_brand_name(row.get('brand_name', ''))
            
            if target_clean and row_brand:
                # Перевіряємо в обидва боки (напр. "SkyUp" в "SkyUp Airlines" або навпаки)
                if target_clean in row_brand or row_brand in target_clean:
                    return True
            return False

        df_full['is_target'] = df_full.apply(check_is_target, axis=1)
        df_full['keyword_text'] = df_full['keyword_id'].map(kw_map) # Додаємо текст запиту
    else:
        df_full = pd.DataFrame()

    # ==============================================================================
    # 4. МЕТРИКИ ПО МОДЕЛЯХ (ПОВЕРНУТО ВАШ КОД)
    # ==============================================================================
    st.markdown("### 🌐 Огляд по моделях")
    
    def get_llm_stats(model_name):
        model_scans = scans_df[scans_df['provider_ui'] == model_name]
        if model_scans.empty: return 0, 0, (0,0,0)
        
        # Беремо найсвіжіші скани по кожному запиту
        latest_scans = model_scans.sort_values('created_at', ascending=False).drop_duplicates('keyword_id')
        target_scan_ids = latest_scans['id'].tolist()
        
        if not target_scan_ids or df_full.empty: return 0, 0, (0,0,0)

        current_mentions = df_full[df_full['scan_result_id'].isin(target_scan_ids)]
        if current_mentions.empty: return 0, 0, (0,0,0)

        total_mentions = current_mentions['mention_count'].sum()
        my_mentions = current_mentions[current_mentions['is_target'] == True]
        my_count = my_mentions['mention_count'].sum()
        
        sov = (my_count / total_mentions * 100) if total_mentions > 0 else 0
        
        valid_ranks = my_mentions[my_mentions['rank_position'] > 0]
        rank = valid_ranks['rank_position'].mean() if not valid_ranks.empty else 0
        
        # Тональність %
        pos, neu, neg = 0, 0, 0
        if not my_mentions.empty:
            total_sent = len(my_mentions)
            pos_c = len(my_mentions[my_mentions['sentiment_score'] == 'Позитивний'])
            neu_c = len(my_mentions[my_mentions['sentiment_score'] == 'Нейтральний'])
            neg_c = len(my_mentions[my_mentions['sentiment_score'] == 'Негативний'])
            
            if total_sent > 0:
                pos = int(pos_c / total_sent * 100)
                neu = int(neu_c / total_sent * 100)
                neg = int(neg_c / total_sent * 100)
                if pos + neu + neg < 100: neu += (100 - (pos+neu+neg))
            
        return sov, rank, (pos, neu, neg)

    cols = st.columns(3)
    models = ['Perplexity', 'OpenAI GPT', 'Google Gemini']
    
    for i, model in enumerate(models):
        with cols[i]:
            sov, rank, (pos, neu, neg) = get_llm_stats(model)
            with st.container(border=True):
                st.markdown(f"**{model}**")
                c1, c2 = st.columns(2)
                
                c1.metric("SOV", f"{sov:.1f}%", help="Share of Voice: Відсоток згадок вашого бренду.")
                c2.metric("Rank", f"#{rank:.1f}" if rank > 0 else "-", help="Середня позиція вашого бренду.")
                
                if pos == 0 and neu == 0 and neg == 0:
                    sent_html = """
                    <div class="sent-container">
                        <div class="sent-header">Тональність <span title="Немає даних">ℹ️</span></div>
                        <div style='color:#ccc; font-size:13px;'>Не знайдено</div>
                    </div>
                    """
                else:
                    sent_html = f"""
                    <div class="sent-container">
                        <div class="sent-header">
                            Тональність 
                            <span title="Розподіл: Позитив / Нейтрал / Негатив" style="cursor:help;">ℹ️</span>
                        </div>
                        <div class="sent-box">
                            <div class="sent-item" style="color:#00C896" title="Позитив">😄 {pos}%</div>
                            <div class="sent-item" style="color:#FFCE56" title="Нейтрал">😐 {neu}%</div>
                            <div class="sent-item" style="color:#FF4B4B" title="Негатив">😡 {neg}%</div>
                        </div>
                    </div>
                    """
                st.markdown(sent_html, unsafe_allow_html=True)

    # ==============================================================================
    # 5. ГРАФІК ДИНАМІКИ (SOV)
    # ==============================================================================
    st.write("")
    st.markdown("### 📈 Динаміка бренду (SOV)")
    st.caption("Графік відображає зміну видимості вашого бренду в часі.")
    
    if not df_full.empty:
        df_full['date_day'] = df_full['created_at'].dt.floor('D')
        daily = df_full.groupby(['date_day', 'provider_ui']).apply(
            lambda x: pd.Series({
                'total': x['mention_count'].sum(),
                'my': x[x['is_target'] == True]['mention_count'].sum()
            })
        ).reset_index()
        daily['sov'] = (daily['my'] / daily['total'] * 100).fillna(0)
        
        fig = px.line(daily, x='date_day', y='sov', color='provider_ui', markers=True, 
                      color_discrete_map={'Perplexity':'#00C896', 'OpenAI GPT':'#FF4B4B', 'Google Gemini':'#3B82F6'})
        fig.update_layout(height=300, margin=dict(l=0,r=0,t=10,b=0), hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Немає даних.")

    # ==============================================================================
    # 6. КОНКУРЕНТНИЙ АНАЛІЗ (ВАШ КОД)
    # ==============================================================================
    st.write("")
    st.markdown("### 🏆 Конкурентний аналіз")
    st.caption("Зведена статистика по всіх моделях. Показники відображаються числами.")

    if not df_full.empty:
        total_mentions_all = df_full['mention_count'].sum()
        total_kws_all = df_full['keyword_id'].nunique()

        df_target_raw = df_full[df_full['is_target'] == True]
        df_competitors_raw = df_full[df_full['is_target'] == False]

        def get_dominant_sentiment(series):
            if series.empty: return "-"
            mode = series.mode()
            return mode[0] if not mode.empty else "Нейтральний"

        # 1. Наш бренд
        if not df_target_raw.empty:
            merged_target = pd.Series({
                'brand_name': f"🟢 {target_brand_raw} (Ви)",
                'mentions': df_target_raw['mention_count'].sum(),
                'unique_kws': df_target_raw['keyword_id'].nunique(),
                'sentiment': get_dominant_sentiment(df_target_raw['sentiment_score']),
                'first_seen': df_target_raw['created_at'].min(),
                'is_target': True
            })
            target_df = pd.DataFrame([merged_target])
        else:
            target_df = pd.DataFrame([{
                'brand_name': f"🟢 {target_brand_raw} (Ви)",
                'mentions': 0, 'unique_kws': 0, 'sentiment': '-', 'first_seen': None, 'is_target': True
            }])

        # 2. Конкуренти
        def agg_competitors(x):
            return pd.Series({
                'mentions': x['mention_count'].sum(),
                'unique_kws': x['keyword_id'].nunique(),
                'sentiment': get_dominant_sentiment(x['sentiment_score']),
                'first_seen': x['created_at'].min(),
                'is_target': False
            })
        
        if not df_competitors_raw.empty:
            competitors_agg = df_competitors_raw.groupby('brand_name').apply(agg_competitors).reset_index()
            competitors_top9 = competitors_agg.sort_values('mentions', ascending=False).head(9)
        else:
            competitors_top9 = pd.DataFrame()

        final_df = pd.concat([target_df, competitors_top9])
        final_df = final_df.sort_values('mentions', ascending=False)

        final_df['sov'] = (final_df['mentions'] / total_mentions_all).fillna(0)
        final_df['presence'] = (final_df['unique_kws'] / total_kws_all).fillna(0)

        rows = []
        for _, r in final_df.iterrows():
            d_str = r['first_seen'].strftime("%d.%m.%Y") if pd.notnull(r['first_seen']) else "-"
            rows.append({
                "Бренд": r['brand_name'], 
                "Згадок": r['mentions'],
                "SOV": r['sov'],
                "Присутність": r['presence'],
                "Тональність": r['sentiment'], 
                "Перша згадка": d_str
            })
            
        st.dataframe(
            pd.DataFrame(rows), 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "Згадок": st.column_config.NumberColumn(format="%d", help="Сумарна кількість згадок."),
                "SOV": st.column_config.NumberColumn("Частка голосу (SOV)", format="%.1f%%", help="% від усіх згадок."),
                "Присутність": st.column_config.NumberColumn("Присутність", format="%.0f%%", help="% запитів, де знайдено бренд."),
                "Тональність": st.column_config.TextColumn("Тональність", help="Домінуюча тональність."),
            }
        )
    else:
        st.info("Немає даних для аналізу конкурентів.")

    # ==============================================================================
    # 7. ДЕТАЛЬНА СТАТИСТИКА (З FIX TIMEZONE)
    # ==============================================================================
    st.write("")
    st.markdown("### 📋 Детальна статистика по запитах")
    st.caption("Метрики розраховані для вашого цільового бренду.")
    
    cols = st.columns([0.4, 2.5, 1, 1, 1, 1.2, 2])
    cols[1].markdown("**Запит**")
    cols[2].markdown("**Згадок**")
    cols[3].markdown("**SOV**")
    cols[4].markdown("**Позиція**")
    cols[5].markdown("**Тональність**")
    cols[6].markdown("**Топ Конкурент / Джерела**")
    
    st.markdown("---")

    unique_kws = keywords_df.to_dict('records')
    
    for idx, kw in enumerate(unique_kws, 1):
        kw_id = kw['id']
        kw_text = kw['keyword_text']
        
        cur_sov, cur_rank, my_mentions_count = 0, 0, 0
        cur_sent = "—"
        top_comp_name, top_comp_val = "—", 0
        off_sources_count = 0
        has_data = False

        if not df_full.empty:
            kw_data = df_full[df_full['keyword_id'] == kw_id]
            
            if not kw_data.empty:
                # 🔥 FIX 3: Таймзони для фільтрації останніх даних
                if kw_data['created_at'].dt.tz is None:
                    kw_data['created_at'] = kw_data['created_at'].dt.tz_localize('UTC')
                
                latest_date = kw_data['created_at'].max()
                current_slice = kw_data[kw_data['created_at'] >= (latest_date - timedelta(hours=2))] # 2 години вікно

                if not current_slice.empty:
                    has_data = True
                    
                    # Наш бренд
                    my_rows = current_slice[current_slice['is_target'] == True]
                    my_mentions_count = my_rows['mention_count'].sum()
                    tot = current_slice['mention_count'].sum()
                    cur_sov = (my_mentions_count / tot * 100) if tot > 0 else 0
                    
                    ranks = my_rows[my_rows['rank_position'] > 0]['rank_position']
                    cur_rank = ranks.mean() if not ranks.empty else 0
                    
                    if not my_rows.empty:
                        cur_sent = my_rows['sentiment_score'].mode()[0]

                    # Конкурент
                    competitors = current_slice[current_slice['is_target'] == False]
                    if not competitors.empty:
                        top_comp_name = competitors.groupby('brand_name')['mention_count'].sum().idxmax()
                        top_comp_val = competitors.groupby('brand_name')['mention_count'].sum().max()
                    
                    # Джерела
                    if not sources_df.empty:
                        scan_ids_kw = current_slice['scan_result_id'].unique()
                        kw_sources = sources_df[sources_df['scan_result_id'].isin(scan_ids_kw)]
                        if 'is_official' in kw_sources.columns:
                            off_sources_count = len(kw_sources[kw_sources['is_official'] == True])

        with st.container():
            c = st.columns([0.4, 2.5, 1, 1, 1, 1.2, 2])
            c[0].markdown(f"<div class='green-number'>{idx}</div>", unsafe_allow_html=True)
            c[1].markdown(f"<span class='kw-row-text'>{kw_text}</span>", unsafe_allow_html=True)
            
            if has_data:
                c[2].markdown(f"**{int(my_mentions_count)}**", unsafe_allow_html=True)
                c[3].markdown(f"{cur_sov:.1f}%", unsafe_allow_html=True)
                c[4].markdown(f"#{cur_rank:.1f}" if cur_rank > 0 else "-", unsafe_allow_html=True)
                
                st_col = "#333"
                if "Поз" in str(cur_sent): st_col = "#00C896"
                elif "Нег" in str(cur_sent): st_col = "#FF4B4B"
                elif "Ней" in str(cur_sent): st_col = "#FFCE56"
                elif "—" in str(cur_sent): st_col = "#ccc"
                
                c[5].markdown(f"<span style='color:{st_col}; font-weight:bold'>{cur_sent}</span>", unsafe_allow_html=True)
                
                # Червоний і великий конкурент
                c[6].markdown(f"""
                <span class='competitor-highlight' title='Головний конкурент'>VS {top_comp_name} ({top_comp_val})</span><br>
                <span class='source-tag' title='Знайдено офіційних посилань'>🔗 Офіц: {off_sources_count}</span>
                """, unsafe_allow_html=True)
            else:
                for i in range(2, 7): c[i].caption("—")
        
        st.markdown("<hr style='margin: 5px 0; border-top: 1px solid #eee;'>", unsafe_allow_html=True)


        
# =========================
# 7. КЕРУВАННЯ ЗАПИТАМИ
# =========================

def show_keyword_details(kw_id):
    """
    Сторінка детальної аналітики одного запиту.
    ВЕРСІЯ: FULL UI + FIXES.
    """
    import pandas as pd
    import plotly.express as px
    import streamlit as st
    from datetime import datetime, timedelta
    import re # <--- Fix
    
    # 0. ПІДКЛЮЧЕННЯ
    if 'supabase' not in globals():
        if 'supabase' in st.session_state:
            supabase = st.session_state['supabase']
        else:
            st.error("🚨 Помилка: Змінна 'supabase' не знайдена.")
            return
    else:
        supabase = globals()['supabase']

    # --- MAPPING ---
    MODEL_CONFIG = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }
    ALL_MODELS_UI = list(MODEL_CONFIG.keys())
    
    def get_ui_model_name(db_name):
        lower = str(db_name).lower()
        if "perplexity" in lower: return "Perplexity"
        if "gpt" in lower or "openai" in lower: return "OpenAI GPT"
        if "gemini" in lower or "google" in lower: return "Google Gemini"
        return db_name 

    def tooltip(text):
        return f'<span title="{text}" style="cursor:help; font-size:14px; color:#333; margin-left:4px;">ℹ️</span>'

    def normalize_url(u):
        u = str(u).strip()
        u = re.split(r'[)\]]', u)[0] # Очистка від Markdown
        if not u.startswith(('http://', 'https://')): return f"https://{u}"
        return u

    # 🔥 FIX: Нормалізація для кирилиці
    def normalize_brand_name(name):
        if not name: return ""
        s = str(name).lower()
        return "".join(c for c in s if c.isalnum())

    # 1. ОТРИМАННЯ ДАНИХ ЗАПИТУ
    try:
        kw_resp = supabase.table("keywords").select("*").eq("id", kw_id).execute()
        if not kw_resp.data:
            st.error("Запит не знайдено.")
            st.session_state["focus_keyword_id"] = None
            st.rerun()
            return
        
        keyword_record = kw_resp.data[0]
        keyword_text = keyword_record["keyword_text"]
        project_id = keyword_record["project_id"]
        
        proj = st.session_state.get("current_project", {})
        target_brand_name = proj.get("brand_name", "").strip()
        
    except Exception as e:
        st.error(f"Помилка БД: {e}")
        return

    # HEADER
    col_back, col_title = st.columns([1, 15])
    with col_back:
        if st.button("⬅", key="back_from_details", help="Назад до списку"):
            st.session_state["focus_keyword_id"] = None
            st.rerun()
    
    with col_title:
        st.markdown(f"<h3 style='margin-top: -5px;'>🔍 {keyword_text}</h3>", unsafe_allow_html=True)

    # ⚙️ БЛОК НАЛАШТУВАНЬ
    with st.expander("⚙️ Налаштування та Нове сканування", expanded=False):
        c1, c2 = st.columns(2)
        
        # ЛІВА: РЕДАГУВАННЯ
        with c1:
            edit_key = f"edit_mode_{kw_id}"
            if edit_key not in st.session_state: st.session_state[edit_key] = False
            
            new_text = st.text_input(
                "Текст запиту", 
                value=keyword_text, 
                key="edit_kw_input",
                disabled=not st.session_state[edit_key]
            )
            
            if not st.session_state[edit_key]:
                if st.button("✏️ Редагувати", key="enable_edit_btn"):
                    st.session_state[edit_key] = True
                    st.rerun()
            else:
                if st.button("💾 Зберегти", key="save_kw_btn"):
                    if new_text and new_text != keyword_text:
                        supabase.table("keywords").update({"keyword_text": new_text}).eq("id", kw_id).execute()
                        st.success("Збережено!")
                    st.session_state[edit_key] = False
                    st.rerun()

        # ПРАВА: ЗАПУСК
        with c2:
            selected_models_to_run = st.multiselect(
                "Оберіть моделі для сканування:", 
                options=ALL_MODELS_UI, 
                default=ALL_MODELS_UI, 
                key="rescan_models_select"
            )
            
            confirm_run_key = f"confirm_run_{kw_id}"
            if confirm_run_key not in st.session_state: st.session_state[confirm_run_key] = False

            if not st.session_state[confirm_run_key]:
                if st.button("🚀 Запустити сканування", key="pre_run_btn"):
                    st.session_state[confirm_run_key] = True
                    st.rerun()
            else:
                c_conf1, c_conf2 = st.columns(2)
                with c_conf1:
                    if st.button("✅ Підтвердити", type="primary", key="real_run_btn"):
                        proj = st.session_state.get("current_project", {})
                        if 'n8n_trigger_analysis' in globals():
                            n8n_trigger_analysis(
                                project_id, 
                                [new_text], 
                                proj.get("brand_name"), 
                                models=selected_models_to_run
                            )
                            st.success("Задачу відправлено! Оновлення даних...")
                            time.sleep(2)
                            st.session_state[confirm_run_key] = False
                            st.rerun()
                        else:
                            st.error("Функція запуску не знайдена.")
                with c_conf2:
                    if st.button("❌ Скасувати", key="cancel_run_btn"):
                        st.session_state[confirm_run_key] = False
                        st.rerun()

    # 2. ОТРИМАННЯ ДАНИХ
    try:
        scans_resp = supabase.table("scan_results")\
            .select("id, created_at, provider, raw_response")\
            .eq("keyword_id", kw_id)\
            .order("created_at", desc=False)\
            .execute()
        
        scans_data = scans_resp.data if scans_resp.data else []
        df_scans = pd.DataFrame(scans_data)
        
        if not df_scans.empty:
            df_scans.rename(columns={'id': 'scan_id'}, inplace=True)
            
            # --- TIMEZONE FIX ---
            df_scans['created_at'] = pd.to_datetime(df_scans['created_at'])
            if df_scans['created_at'].dt.tz is None:
                df_scans['created_at'] = df_scans['created_at'].dt.tz_localize('UTC')
            
            try: df_scans['date_str'] = df_scans['created_at'].dt.tz_convert('Europe/Kiev').dt.strftime('%Y-%m-%d %H:%M')
            except: df_scans['date_str'] = df_scans['created_at'].dt.strftime('%Y-%m-%d %H:%M')
            
            df_scans['provider_ui'] = df_scans['provider'].apply(get_ui_model_name)
        else:
            df_scans = pd.DataFrame(columns=['scan_id', 'created_at', 'provider', 'raw_response', 'date_str', 'provider_ui'])

        # B. Mentions
        df_mentions = pd.DataFrame()
        if not df_scans.empty:
            scan_ids = df_scans['scan_id'].tolist()
            if scan_ids:
                mentions_resp = supabase.table("brand_mentions").select("*").in_("scan_result_id", scan_ids).execute()
                if mentions_resp.data: df_mentions = pd.DataFrame(mentions_resp.data)

        # 🔥 FIX: SMART MERGE (Дублікати & Кирилиця)
        if not df_mentions.empty:
            target_clean = normalize_brand_name(target_brand_name)
            
            def check_is_real_target(row):
                if row.get('is_my_brand') is True: return True
                
                row_brand = normalize_brand_name(row.get('brand_name', ''))
                if target_clean and row_brand:
                    if target_clean in row_brand or row_brand in target_clean:
                        return True
                return False

            df_mentions['is_real_target'] = df_mentions.apply(check_is_real_target, axis=1)
            
            # Merge
            df_full = pd.merge(df_scans, df_mentions, left_on='scan_id', right_on='scan_result_id', how='left')
        else:
            df_full = df_scans.copy()
            df_full['mention_count'] = 0
            df_full['is_real_target'] = False
            df_full['scan_result_id'] = df_full['scan_id'] if not df_full.empty else None
            df_full['sentiment_score'] = None
            df_full['rank_position'] = None
            df_full['brand_name'] = None

    except Exception as e:
        st.error(f"Помилка обробки даних: {e}")
        return

    # 5. ДЕТАЛІЗАЦІЯ (TABS)
    st.markdown("##### 📝 Детальний аналіз відповідей")
    
    tabs = st.tabs(ALL_MODELS_UI)
    
    for tab, ui_model_name in zip(tabs, ALL_MODELS_UI):
        with tab:
            if not df_scans.empty:
                model_scans = df_scans[df_scans['provider_ui'] == ui_model_name].sort_values('created_at', ascending=False)
            else:
                model_scans = pd.DataFrame()
            
            if model_scans.empty:
                st.write(f"📉 Даних від **{ui_model_name}** ще немає.")
                continue

            with st.container(border=True):
                scan_options = {row['date_str']: row['scan_id'] for _, row in model_scans.iterrows()}
                
                c_sel, c_del = st.columns([3, 1])
                with c_sel:
                    selected_date = st.selectbox(
                        f"Оберіть дату аналізу ({ui_model_name}):", 
                        list(scan_options.keys()), 
                        key=f"sel_date_{ui_model_name}"
                    )
                
                selected_scan_id = scan_options[selected_date]
                
                with c_del:
                    st.write(""); st.write("")
                    confirm_key = f"del_scan_{selected_scan_id}"
                    if confirm_key not in st.session_state: st.session_state[confirm_key] = False

                    if not st.session_state[confirm_key]:
                        if st.button("🗑️ Видалити", key=f"btn_del_{selected_scan_id}"):
                            st.session_state[confirm_key] = True
                            st.rerun()
                    else:
                        c_y, c_n = st.columns(2)
                        if c_y.button("✅", key=f"yes_{selected_scan_id}"):
                            try:
                                supabase.table("scan_results").delete().eq("id", selected_scan_id).execute()
                                st.success("Видалено!")
                                st.session_state[confirm_key] = False
                                time.sleep(1)
                                st.rerun()
                            except Exception as e:
                                st.error(f"Помилка: {e}")
                        
                        if c_n.button("❌", key=f"no_{selected_scan_id}"):
                            st.session_state[confirm_key] = False
                            st.rerun()

            current_scan_row = model_scans[model_scans['scan_id'] == selected_scan_id].iloc[0]
            
            # --- LOCAL METRICS ---
            loc_sov, loc_mentions = 0, 0
            loc_sent = "Не згадано"
            loc_rank_str = "-"
            
            current_scan_mentions = pd.DataFrame()
            if not df_mentions.empty:
                current_scan_mentions = df_mentions[df_mentions['scan_result_id'] == selected_scan_id]
            
            if not current_scan_mentions.empty:
                total_in_scan = current_scan_mentions['mention_count'].sum()
                
                # 🔥 FIX: Фільтруємо ВСІ рядки, що підходять під "Мій Бренд"
                my_brand_rows = current_scan_mentions[current_scan_mentions['is_real_target'] == True]

                if not my_brand_rows.empty:
                    val_my_mentions = my_brand_rows['mention_count'].sum()
                    valid_ranks = my_brand_rows[my_brand_rows['rank_position'] > 0]['rank_position']
                    val_rank = valid_ranks.min() if not valid_ranks.empty else None
                    
                    if val_my_mentions > 0:
                        main_row = my_brand_rows.sort_values('mention_count', ascending=False).iloc[0]
                        loc_sent = main_row['sentiment_score']
                    
                    loc_mentions = int(val_my_mentions)
                    loc_sov = (val_my_mentions / total_in_scan * 100) if total_in_scan > 0 else 0
                    loc_rank_str = f"#{val_rank:.0f}" if pd.notna(val_rank) else "-"
            
            sent_color = "#333"
            if loc_sent == "Позитивний": sent_color = "#00C896"
            elif loc_sent == "Негативний": sent_color = "#FF4B4B"
            elif loc_sent == "Не знайдено": sent_color = "#999"

            st.markdown(f"""
            <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-bottom: 20px;">
                <div style="background:#fff; border:1px solid #E0E0E0; border-top:4px solid #00C896; border-radius:8px; padding:15px; text-align:center;">
                    <div style="font-size:11px; color:#888; font-weight:600;">ЧАСТКА ГОЛОСУ (SOV) {tooltip('Відсоток згадок вашого бренду в цій конкретній відповіді.')}</div>
                    <div style="font-size:24px; font-weight:700; color:#333;">{loc_sov:.1f}%</div>
                </div>
                <div style="background:#fff; border:1px solid #E0E0E0; border-top:4px solid #00C896; border-radius:8px; padding:15px; text-align:center;">
                    <div style="font-size:11px; color:#888; font-weight:600;">ЗГАДОК БРЕНДУ {tooltip('Кількість разів, коли бренд був згаданий.')}</div>
                    <div style="font-size:24px; font-weight:700; color:#333;">{loc_mentions}</div>
                </div>
                <div style="background:#fff; border:1px solid #E0E0E0; border-top:4px solid #00C896; border-radius:8px; padding:15px; text-align:center;">
                    <div style="font-size:11px; color:#888; font-weight:600;">ТОНАЛЬНІСТЬ {tooltip('Емоційне забарвлення згадки в цій відповіді.')}</div>
                    <div style="font-size:18px; font-weight:600; color:{sent_color}; margin-top:5px;">{loc_sent}</div>
                </div>
                <div style="background:#fff; border:1px solid #E0E0E0; border-top:4px solid #00C896; border-radius:8px; padding:15px; text-align:center;">
                    <div style="font-size:11px; color:#888; font-weight:600;">ПОЗИЦІЯ У СПИСКУ {tooltip('Порядковий номер першої згадки бренду.')}</div>
                    <div style="font-size:24px; font-weight:700; color:#333;">{loc_rank_str}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            raw_text = current_scan_row.get('raw_response', '')
            st.markdown("##### Відповідь від LLM")
            
            if raw_text:
                final_html = raw_text
                if target_brand_name:
                    highlight_span = f"<span style='background-color:#dcfce7; color:#166534; font-weight:bold; padding:0 4px; border-radius:4px;'>{target_brand_name}</span>"
                    try: final_html = re.sub(re.escape(target_brand_name), highlight_span, final_html, flags=re.IGNORECASE)
                    except: pass
                st.markdown(f"""<div style="background-color: #f9fffb; border: 1px solid #bbf7d0; border-radius: 8px; padding: 20px; font-size: 16px; line-height: 1.6; color: #374151;">{final_html}</div>""", unsafe_allow_html=True)
            else:
                st.info("Текст відповіді не збережено.")

            st.markdown("<br>", unsafe_allow_html=True)

            # --- БРЕНДИ ---
            st.markdown(f"**Знайдені бренди:** {tooltip('Бренди, які AI згадав у цій відповіді.')}", unsafe_allow_html=True)
            
            if not current_scan_mentions.empty:
                scan_mentions_plot = current_scan_mentions[current_scan_mentions['mention_count'] > 0].copy()
                scan_mentions_plot = scan_mentions_plot.sort_values('mention_count', ascending=False)

                if not scan_mentions_plot.empty:
                    c_chart, c_table = st.columns([1.3, 2], vertical_alignment="center")
                    with c_chart:
                        fig_brands = px.pie(
                            scan_mentions_plot, values='mention_count', names='brand_name', hole=0.5,
                            color_discrete_sequence=px.colors.qualitative.Pastel,
                            labels={'brand_name': 'Бренд', 'mention_count': 'Згадок'}
                        )
                        fig_brands.update_traces(textposition='inside', textinfo='percent+label', hovertemplate='<b>%{label}</b><br>Згадок: %{value}')
                        fig_brands.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), height=250)
                        st.plotly_chart(fig_brands, use_container_width=True, config={'displayModeBar': False})
                    with c_table:
                        st.dataframe(
                            scan_mentions_plot[['brand_name', 'mention_count', 'rank_position', 'sentiment_score']],
                            column_config={
                                "brand_name": "Бренд",
                                "mention_count": st.column_config.NumberColumn("Згадок"),
                                "rank_position": st.column_config.NumberColumn("Позиція"),
                                "sentiment_score": st.column_config.TextColumn("Тональність")
                            },
                            use_container_width=True, hide_index=True
                        )
                else:
                     st.info("Брендів не знайдено.")
            else:
                st.info("Брендів не знайдено.")
            
            st.markdown("<br>", unsafe_allow_html=True)

            # --- ДЖЕРЕЛА ---
            st.markdown(f"#### 🔗 Цитовані джерела {tooltip('Посилання, які надала модель.')}", unsafe_allow_html=True)
            try:
                sources_resp = supabase.table("extracted_sources").select("*").eq("scan_result_id", selected_scan_id).execute()
                sources_data = sources_resp.data
                if sources_data:
                    df_src = pd.DataFrame(sources_data)
                    
                    if 'url' in df_src.columns:
                        if 'domain' not in df_src.columns:
                            from urllib.parse import urlparse
                            df_src['domain'] = df_src['url'].apply(lambda x: str(x).split('/')[2] if x and '//' in str(x) else 'unknown')
                        
                        df_src['url'] = df_src['url'].apply(normalize_url)
                        
                        if 'is_official' in df_src.columns:
                            df_src['status_text'] = df_src['is_official'].apply(lambda x: "✅ Офіційне" if x is True else "🔗 Зовнішнє")
                        else:
                            df_src['status_text'] = "🔗 Зовнішнє"

                        # ГРУПУВАННЯ
                        df_grouped_src = df_src.groupby(['url', 'domain', 'status_text'], as_index=False).size()
                        df_grouped_src = df_grouped_src.rename(columns={'size': 'count'})
                        df_grouped_src = df_grouped_src.sort_values(by='count', ascending=False)

                        c_src_chart, c_src_table = st.columns([1.3, 2], vertical_alignment="center")
                        
                        with c_src_chart:
                            domain_counts = df_grouped_src.groupby('domain')['count'].sum().reset_index()
                            fig_src = px.pie(
                                domain_counts.head(10), values='count', names='domain', hole=0.5,
                                labels={'domain': 'Домен', 'count': 'Кількість'}
                            )
                            fig_src.update_traces(textposition='inside', textinfo='percent', hovertemplate='<b>%{label}</b><br>Кількість: %{value}')
                            fig_src.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), height=200)
                            st.plotly_chart(fig_src, use_container_width=True, config={'displayModeBar': False})

                        with c_src_table:
                            st.dataframe(
                                df_grouped_src[['url', 'status_text', 'count']], 
                                use_container_width=True, 
                                hide_index=True,
                                column_config={
                                    "url": st.column_config.LinkColumn("Посилання", width="large", validate="^https?://"),
                                    "status_text": st.column_config.TextColumn("Тип", width="small"),
                                    "count": st.column_config.NumberColumn("К-сть", width="small")
                                }
                            )
                    else:
                        st.info("URL не знайдено.")
                else:
                    st.info("ℹ️ Джерел не знайдено.")
            except Exception as e:
                st.error(f"Помилка завантаження джерел: {e}")



def show_keywords_page():
    """
    Сторінка списку запитів.
    ВЕРСІЯ: FIX DUPLICATE KEY ERROR.
    1. Виправлено DuplicateElementKey (додано idx до ключів чекбоксів).
    2. Синхронізація дозволів автосканування.
    """
    import pandas as pd
    import streamlit as st
    from datetime import datetime
    import time
    import io 
    import re 
    
    # CSS Стилізація
    st.markdown("""
    <style>
        .green-number {
            background-color: #00C896;
            color: white;
            width: 28px;
            height: 28px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: bold;
            font-size: 14px;
            margin-top: 5px; 
        }
        div[data-testid="stColumn"]:nth-of-type(3) button[kind="secondary"] {
            border: none;
            background: transparent;
            text-align: left;
            padding-left: 0;
            font-weight: 600;
            color: #31333F;
            box-shadow: none;
        }
        div[data-testid="stColumn"]:nth-of-type(3) button[kind="secondary"]:hover {
            color: #00C896;
            background: transparent;
            border: none;
            box-shadow: none;
        }
        div[data-testid="stColumn"]:nth-of-type(3) button[kind="secondary"]:active {
            color: #00C896;
            background: transparent;
            box-shadow: none;
        }
    </style>
    """, unsafe_allow_html=True)

    try:
        import pytz
        kyiv_tz = pytz.timezone('Europe/Kiev')
    except ImportError:
        kyiv_tz = None

    MODEL_MAPPING = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }

    if 'supabase' not in globals():
        if 'supabase' in st.session_state:
            supabase = st.session_state['supabase']
        else:
            st.error("🚨 Помилка: Змінна 'supabase' не знайдена.")
            return
    else:
        supabase = globals()['supabase']

    if "kw_input_count" not in st.session_state:
        st.session_state["kw_input_count"] = 1

    # --- СИНХРОНІЗАЦІЯ З БД ---
    if "current_project" in st.session_state and st.session_state["current_project"]:
        try:
            curr_id = st.session_state["current_project"]["id"]
            refresh_resp = supabase.table("projects").select("*").eq("id", curr_id).execute()
            if refresh_resp.data:
                st.session_state["current_project"] = refresh_resp.data[0]
        except Exception:
            pass 

    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект в онбордингу.")
        return

    if st.session_state.get("focus_keyword_id"):
        if 'show_keyword_details' in globals():
            show_keyword_details(st.session_state["focus_keyword_id"])
            return

    st.markdown("<h3 style='padding-top:0;'>📋 Перелік запитів</h3>", unsafe_allow_html=True)

    def format_kyiv_time(iso_str):
        if not iso_str or iso_str == "1970-01-01T00:00:00+00:00":
            return "—"
        try:
            dt_utc = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
            if kyiv_tz:
                dt_kyiv = dt_utc.astimezone(kyiv_tz)
                return dt_kyiv.strftime("%d.%m %H:%M")
            else:
                return dt_utc.strftime("%d.%m %H:%M UTC")
        except:
            return iso_str

    def update_kw_field(kw_id, field, value):
        try:
            supabase.table("keywords").update({field: value}).eq("id", kw_id).execute()
        except Exception as e:
            st.error(f"Помилка оновлення: {e}")

    # ========================================================
    # 2. БЛОК РЕДАГУВАННЯ
    # ========================================================
    with st.expander("✏️ Редагування запитів", expanded=False): 
        
        tab_manual, tab_import, tab_export = st.tabs(["✍️ Ввести вручну", "📥 Імпорт (Excel / URL)", "📤 Експорт (Excel)"])

        # --- TAB 1: ВРУЧНУ ---
        with tab_manual:
            with st.container(border=True):
                st.markdown("##### 📝 Введіть нові запити")
                for i in range(st.session_state["kw_input_count"]):
                    st.text_input(f"Запит #{i+1}", key=f"new_kw_input_{i}", placeholder="Наприклад: Купити квитки...")

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
                c_models, c_submit = st.columns([3, 1])
                with c_models:
                    selected_models_manual = st.multiselect("LLM для першого скану:", list(MODEL_MAPPING.keys()), default=["Perplexity"], key="manual_multiselect")
                
                with c_submit:
                    st.write("")
                    st.write("")
                    if st.button("🚀 Додати", use_container_width=True, type="primary", key="btn_add_manual"):
                        new_keywords_list = []
                        for i in range(st.session_state["kw_input_count"]):
                            val = st.session_state.get(f"new_kw_input_{i}", "").strip()
                            if val: new_keywords_list.append(val)
                        
                        if new_keywords_list:
                            try:
                                insert_data = [{
                                    "project_id": proj["id"], "keyword_text": kw, "is_active": True, 
                                    "is_auto_scan": False, "frequency": "daily"
                                } for kw in new_keywords_list]
                                
                                res = supabase.table("keywords").insert(insert_data).execute()
                                if res.data:
                                    with st.spinner(f"Зберігаємо та запускаємо аналіз..."):
                                        if 'n8n_trigger_analysis' in globals():
                                            for new_kw in new_keywords_list:
                                                n8n_trigger_analysis(proj["id"], [new_kw], proj.get("brand_name"), models=selected_models_manual)
                                                time.sleep(0.5) 
                                    st.success(f"Додано {len(new_keywords_list)} запитів!")
                                    st.session_state["kw_input_count"] = 1
                                    for key in list(st.session_state.keys()):
                                        if key.startswith("new_kw_input_"): del st.session_state[key]
                                    time.sleep(1)
                                    st.rerun()
                            except Exception as e:
                                st.error(f"Помилка: {e}")
                        else:
                            st.warning("Введіть хоча б один запит.")

        # --- TAB 2: ІМПОРТ EXCEL / URL ---
        with tab_import:
            st.info("💡 Завантажте файл .xlsx або вставте посилання на Google Sheet. **Важливо:** Для Google Sheet має бути відкрито доступ (Anyone with the link). Перша колонка має називатися **Keyword**.")
            
            import_source = st.radio("Джерело:", ["Файл (.xlsx)", "Посилання (URL)"], horizontal=True)
            df_upload = None
            
            if import_source == "Файл (.xlsx)":
                uploaded_file = st.file_uploader("Оберіть файл Excel", type=["xlsx"])
                if uploaded_file:
                    try:
                        df_upload = pd.read_excel(uploaded_file)
                    except ImportError:
                        st.error("🚨 Відсутня бібліотека `openpyxl`. Будь ласка, додайте `openpyxl` у requirements.txt вашого проекту.")
                    except Exception as e:
                        st.error(f"Не вдалося прочитати файл: {e}")
            else: # URL
                import_url = st.text_input("Вставте посилання (Google Sheets або CSV):")
                if import_url:
                    try:
                        if "docs.google.com" in import_url:
                            match = re.search(r'/d/([a-zA-Z0-9-_]+)', import_url)
                            if match:
                                sheet_id = match.group(1)
                                csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
                                df_upload = pd.read_csv(csv_url)
                            else:
                                st.error("Не вдалося розпізнати ID Google таблиці. Перевірте посилання.")
                        elif import_url.endswith(".csv"):
                            df_upload = pd.read_csv(import_url)
                        elif import_url.endswith(".xlsx"):
                            df_upload = pd.read_excel(import_url)
                        else:
                            st.warning("Спробуємо прочитати як CSV...")
                            df_upload = pd.read_csv(import_url)
                    except Exception as e:
                        if "400" in str(e) or "403" in str(e):
                            st.error("🔒 Помилка доступу (HTTP 400/403).")
                        else:
                            st.error(f"Не вдалося завантажити: {e}")

            if df_upload is not None:
                target_col = None
                cols_lower = [str(c).lower().strip() for c in df_upload.columns]
                
                if "keyword" in cols_lower:
                    target_col = df_upload.columns[cols_lower.index("keyword")]
                elif "запит" in cols_lower:
                    target_col = df_upload.columns[cols_lower.index("запит")]
                else:
                    target_col = df_upload.columns[0] 
                
                preview_kws = df_upload[target_col].dropna().astype(str).tolist()
                st.write(f"✅ Знайдено **{len(preview_kws)}** запитів. Приклад: {preview_kws[:3]}")
                
                st.write("---")
                st.write("Оберіть дію:")
                
                c_imp_models, c_imp_btn1, c_imp_btn2 = st.columns([2, 1.5, 1.5])
                
                with c_imp_models:
                    selected_models_import = st.multiselect("LLM (тільки для кнопки аналізу):", list(MODEL_MAPPING.keys()), default=["Perplexity"], key="import_multiselect")
                
                with c_imp_btn1:
                    st.write("")
                    st.write("")
                    if st.button("📥 Тільки зберегти", use_container_width=True):
                        if preview_kws:
                            try:
                                insert_data = [{
                                    "project_id": proj["id"], "keyword_text": kw, "is_active": True, 
                                    "is_auto_scan": False, "frequency": "daily"
                                } for kw in preview_kws]
                                
                                supabase.table("keywords").insert(insert_data).execute()
                                st.success(f"Успішно збережено {len(preview_kws)} запитів!")
                                time.sleep(1.5)
                                st.rerun()
                            except Exception as e:
                                st.error(f"Помилка збереження: {e}")

                with c_imp_btn2:
                    st.write("")
                    st.write("")
                    if st.button("🚀 Зберегти та Аналізувати", type="primary", use_container_width=True):
                        if preview_kws:
                            try:
                                insert_data = [{
                                    "project_id": proj["id"], "keyword_text": kw, "is_active": True, 
                                    "is_auto_scan": False, "frequency": "daily"
                                } for kw in preview_kws]
                                
                                res = supabase.table("keywords").insert(insert_data).execute()
                                if res.data:
                                    with st.spinner(f"Обробка {len(preview_kws)} запитів..."):
                                        if 'n8n_trigger_analysis' in globals():
                                            my_bar = st.progress(0, text="Запуск...")
                                            total = len(preview_kws)
                                            for i, kw in enumerate(preview_kws):
                                                n8n_trigger_analysis(proj["id"], [kw], proj.get("brand_name"), models=selected_models_import)
                                                my_bar.progress((i + 1) / total)
                                                time.sleep(0.3)
                                    st.success("Успішно збережено та запущено!")
                                    time.sleep(2)
                                    st.rerun()
                            except Exception as e:
                                st.error(f"Помилка процесу: {e}")

        # --- TAB 3: ЕКСПОРТ EXCEL ---
        with tab_export:
            st.write("Натисніть кнопку нижче, щоб завантажити всі запити цього проекту в Excel.")
            try:
                kws_resp = supabase.table("keywords").select("id, keyword_text, created_at").eq("project_id", proj["id"]).execute()
                if kws_resp.data:
                    df_export = pd.DataFrame(kws_resp.data)
                    scan_resp = supabase.table("scan_results").select("keyword_id, created_at").eq("project_id", proj["id"]).order("created_at", desc=True).execute()
                    
                    last_scan_map = {}
                    if scan_resp.data:
                        for s in scan_resp.data:
                            if s['keyword_id'] not in last_scan_map:
                                last_scan_map[s['keyword_id']] = s['created_at']
                    
                    df_export['last_scan_date'] = df_export['id'].map(lambda x: last_scan_map.get(x, "-"))
                    df_export['created_at'] = pd.to_datetime(df_export['created_at']).dt.strftime('%Y-%m-%d %H:%M')
                    df_export['last_scan_date'] = df_export['last_scan_date'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d %H:%M') if x != "-" else "-")
                    
                    df_final = df_export[["keyword_text", "created_at", "last_scan_date"]].rename(columns={"keyword_text": "Keyword", "created_at": "Date Added", "last_scan_date": "Last Scan Date"})
                    
                    buffer = io.BytesIO()
                    try:
                        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                            df_final.to_excel(writer, index=False, sheet_name='Keywords')
                    except:
                         try:
                             with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                                 df_final.to_excel(writer, index=False, sheet_name='Keywords')
                         except ImportError:
                             st.error("Для експорту потрібна бібліотека `xlsxwriter` або `openpyxl`.")
                             buffer = None

                    if buffer:
                        st.download_button(label="📥 Завантажити Excel", data=buffer.getvalue(), file_name=f"keywords_{proj.get('brand_name')}.xlsx", mime="application/vnd.ms-excel", type="primary")
                else:
                    st.warning("У проекті ще немає запитів для експорту.")
            except Exception as e:
                st.error(f"Помилка підготовки експорту: {e}")

    st.divider()
    
    # ========================================================
    # 3. ОТРИМАННЯ ДАНИХ (ДЛЯ ТАБЛИЦІ НИЖЧЕ)
    # ========================================================
    try:
        keywords = supabase.table("keywords").select("*").eq("project_id", proj["id"]).order("created_at", desc=True).execute().data
        last_scans_resp = supabase.table("scan_results").select("keyword_id, created_at").eq("project_id", proj["id"]).order("created_at", desc=True).execute()
        
        last_scan_map = {}
        if last_scans_resp.data:
            for s in last_scans_resp.data:
                if s['keyword_id'] not in last_scan_map:
                    last_scan_map[s['keyword_id']] = s['created_at']
        
        for k in keywords:
            k['last_scan_date'] = last_scan_map.get(k['id'], "1970-01-01T00:00:00+00:00")

    except Exception as e:
        st.error(f"Помилка завантаження: {e}")
        keywords = []

    if not keywords:
        st.info("Список порожній.")
        return

    # ========================================================
    # 4. ПАНЕЛЬ УПРАВЛІННЯ (СОРТУВАННЯ)
    # ========================================================
    c_sort, _ = st.columns([2, 4])
    with c_sort:
        sort_option = st.selectbox("Сортувати за:", ["Найновіші (Додані)", "Найстаріші (Додані)", "Нещодавно проскановані", "Давно не скановані"], label_visibility="collapsed")

    if sort_option == "Найновіші (Додані)": keywords.sort(key=lambda x: x['created_at'], reverse=True)
    elif sort_option == "Найстаріші (Додані)": keywords.sort(key=lambda x: x['created_at'], reverse=False)
    elif sort_option == "Нещодавно проскановані": keywords.sort(key=lambda x: x['last_scan_date'], reverse=True)
    elif sort_option == "Давно не скановані": keywords.sort(key=lambda x: x['last_scan_date'], reverse=False)

    with st.container(border=True):
        c_check, c_models, c_btn = st.columns([0.5, 3, 1.5])
        with c_check:
            st.write("") 
            select_all = st.checkbox("Всі", key="select_all_kws")
        with c_models:
            bulk_models = st.multiselect("ЛЛМ для запуску:", list(MODEL_MAPPING.keys()), default=["Perplexity"], label_visibility="collapsed", key="bulk_models_main")
        with c_btn:
            if st.button("🚀 Аналізувати обрані", use_container_width=True, type="primary"):
                selected_kws_text = []
                if select_all:
                    selected_kws_text = [k['keyword_text'] for k in keywords]
                else:
                    # 🔥 FIX: Збір обраних з унікальним ключем
                    for idx, k in enumerate(keywords, start=1):
                        if st.session_state.get(f"chk_{k['id']}_{idx}", False):
                            selected_kws_text.append(k['keyword_text'])
                
                if selected_kws_text:
                    my_bar = st.progress(0, text="Ініціалізація...")
                    total_kws = len(selected_kws_text)
                    try:
                        if 'n8n_trigger_analysis' in globals():
                            for i, single_kw in enumerate(selected_kws_text):
                                my_bar.progress((i / total_kws), text=f"Відправка: {single_kw}...")
                                n8n_trigger_analysis(proj["id"], [single_kw], proj.get("brand_name"), models=bulk_models)
                                time.sleep(0.3)
                            my_bar.progress(1.0, text="Готово!")
                            st.success(f"Успішно запущено {total_kws} завдань! Оновіть сторінку за хвилину.")
                            time.sleep(2)
                            st.rerun()
                        else:
                            st.error("Функція запуску не знайдена.")
                    except Exception as e:
                        st.error(f"Помилка при запуску: {e}")
                else:
                    st.warning("Оберіть хоча б один запит.")

    # ========================================================
    # 5. СПИСОК ЗАПИТІВ (ТАБЛИЦЯ)
    # ========================================================
    
    h_chk, h_num, h_txt, h_cron, h_date, h_act = st.columns([0.4, 0.5, 3.2, 2, 1.2, 1.3])
    h_txt.markdown("**Запит**")
    h_cron.markdown("**Автозапуск**")
    h_date.markdown("**Останній аналіз**")
    h_act.markdown("**Видалити**")

    # 🔥 ОТРИМУЄМО ДОЗВІЛ ВІД АДМІНА
    allow_cron_global = proj.get('allow_cron', False)

    for idx, k in enumerate(keywords, start=1):
        with st.container(border=True):
            c1, c2, c3, c4, c5, c6 = st.columns([0.4, 0.5, 3.2, 2, 1.2, 1.3])
            
            with c1:
                st.write("") 
                is_checked = select_all
                # 🔥 FIX: Унікальний ключ з idx
                st.checkbox("", key=f"chk_{k['id']}_{idx}", value=is_checked)
            
            with c2:
                st.markdown(f"<div class='green-number'>{idx}</div>", unsafe_allow_html=True)
            
            with c3:
                if st.button(k['keyword_text'], key=f"link_btn_{k['id']}_{idx}", help="Натисніть для детального аналізу"):
                    st.session_state["focus_keyword_id"] = k["id"]
                    st.rerun()
            
            with c4:
                cron_c1, cron_c2 = st.columns([0.8, 1.2])
                is_auto = k.get('is_auto_scan', False) 
                
                # Ініціалізація перед перевіркою
                new_auto = is_auto 

                with cron_c1:
                    # Відображаємо тогл тільки якщо дозволено глобально
                    if allow_cron_global:
                        new_auto = st.toggle("Авто", value=is_auto, key=f"auto_{k['id']}_{idx}", label_visibility="collapsed")
                        if new_auto != is_auto:
                            update_kw_field(k['id'], "is_auto_scan", new_auto)
                            st.rerun()
                    else:
                        st.toggle("Авто", value=False, key=f"auto_{k['id']}_{idx}", label_visibility="collapsed", disabled=True)
                        st.caption("🔒 Admin")

                with cron_c2:
                    if new_auto and allow_cron_global:
                        current_freq = k.get('frequency', 'daily')
                        freq_options = ["daily", "weekly", "monthly"]
                        try: idx_f = freq_options.index(current_freq)
                        except: idx_f = 0
                        new_freq = st.selectbox("Freq", freq_options, index=idx_f, key=f"freq_{k['id']}_{idx}", label_visibility="collapsed")
                        if new_freq != current_freq:
                            update_kw_field(k['id'], "frequency", new_freq)
                    else:
                        st.write("")
            
            with c5:
                st.write("")
                date_iso = k.get('last_scan_date')
                formatted_date = format_kyiv_time(date_iso)
                st.caption(f"{formatted_date}")
            
            with c6:
                st.write("")
                del_key = f"confirm_del_kw_{k['id']}_{idx}"
                if del_key not in st.session_state: st.session_state[del_key] = False

                if not st.session_state[del_key]:
                    if st.button("🗑️ Видалити", key=f"pre_del_{k['id']}_{idx}"):
                        st.session_state[del_key] = True
                        st.rerun()
                else:
                    dc1, dc2 = st.columns(2)
                    if dc1.button("✅", key=f"yes_del_{k['id']}_{idx}", type="primary"):
                        try:
                            supabase.table("scan_results").delete().eq("keyword_id", k["id"]).execute()
                            supabase.table("keywords").delete().eq("id", k["id"]).execute()
                            st.success("!")
                            st.session_state[del_key] = False
                            time.sleep(0.5)
                            st.rerun()
                        except Exception as e:
                            st.error("Помилка")
                    
                    if dc2.button("❌", key=f"no_del_{k['id']}_{idx}"):
                        st.session_state[del_key] = False
                        st.rerun()




# =========================
# 9. SIDEBAR
# =========================

def show_sources_page():
    """
    Сторінка джерел.
    ВЕРСІЯ: FIXED & REDESIGNED.
    1. Читає з таблиці official_assets (тепер домен з реєстрації буде видно).
    2. Дизайн таблиці Whitelist приведено до стилю інших таблиць.
    3. Виправлено логіку збереження при редагуванні.
    """
    import pandas as pd
    import plotly.express as px
    import streamlit as st
    import time
    from urllib.parse import urlparse
    
    # Підключення
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
        st.info("Спочатку оберіть проект.")
        return

    st.title("🔗 Джерела")

    # ==============================================================================
    # 1. ОТРИМАННЯ ДАНИХ (Скан результати)
    # ==============================================================================
    try:
        # Keywords
        kw_resp = supabase.table("keywords").select("id, keyword_text").eq("project_id", proj["id"]).execute()
        kw_map = {k['id']: k['keyword_text'] for k in kw_resp.data} if kw_resp.data else {}

        # Scan Results
        scan_resp = supabase.table("scan_results")\
            .select("id, provider, created_at, keyword_id")\
            .eq("project_id", proj["id"])\
            .execute()
        
        scan_meta = {} 
        scan_ids = []
        
        PROVIDER_MAP = {
            "perplexity": "Perplexity",
            "gpt-4o": "OpenAI GPT", "gpt-4": "OpenAI GPT",
            "gemini-1.5-pro": "Google Gemini", "gemini": "Google Gemini"
        }

        if scan_resp.data:
            for s in scan_resp.data:
                scan_ids.append(s['id'])
                raw_p = s.get('provider', '').lower()
                clean_p = "Інше"
                for k, v in PROVIDER_MAP.items():
                    if k in raw_p:
                        clean_p = v
                        break
                
                scan_meta[s['id']] = {
                    'provider': clean_p,
                    'date': s['created_at'],
                    'keyword_text': kw_map.get(s['keyword_id'], "Невідомий запит")
                }
        
        # Extracted Sources
        df_master = pd.DataFrame()
        if scan_ids:
            sources_resp = supabase.table("extracted_sources").select("*").in_("scan_result_id", scan_ids).execute()
            if sources_resp.data:
                df_master = pd.DataFrame(sources_resp.data)
                df_master['provider'] = df_master['scan_result_id'].map(lambda x: scan_meta.get(x, {}).get('provider', 'Інше'))
                df_master['keyword_text'] = df_master['scan_result_id'].map(lambda x: scan_meta.get(x, {}).get('keyword_text', ''))
                df_master['scan_date'] = df_master['scan_result_id'].map(lambda x: scan_meta.get(x, {}).get('date'))
                
                if 'domain' not in df_master.columns:
                    df_master['domain'] = df_master['url'].apply(lambda x: urlparse(x).netloc if x else "unknown")

    except Exception as e:
        st.error(f"Помилка завантаження даних: {e}")
        df_master = pd.DataFrame()

    # ==============================================================================
    # 2. WHITELIST LOGIC (ПРАВИЛЬНЕ ЧИТАННЯ)
    # ==============================================================================
    try:
        # 🔥 FIX: Читаємо з таблиці official_assets
        oa_resp = supabase.table("official_assets").select("domain_or_url, type").eq("project_id", proj["id"]).execute()
        raw_assets = oa_resp.data if oa_resp.data else []
    except Exception as e:
        raw_assets = []

    # Формуємо список для логіки
    assets_list_dicts = []
    for item in raw_assets:
        assets_list_dicts.append({
            "Домен": item.get("domain_or_url", ""), 
            "Мітка": item.get("type", "Веб-сайт")
        })
    
    OFFICIAL_DOMAINS = [d["Домен"].lower().strip() for d in assets_list_dicts if d["Домен"]]

    # Функція перевірки
    def check_is_official(url):
        if not url: return False
        u_str = str(url).lower()
        for od in OFFICIAL_DOMAINS:
            if od in u_str: return True
        return False

    if not df_master.empty:
        df_master['is_official_dynamic'] = df_master['url'].apply(check_is_official)

    # ==============================================================================
    # 3. ВКЛАДКИ
    # ==============================================================================
    tab1, tab2, tab3 = st.tabs(["📊 Офіційні ресурси бренду", "🌐 Ренкінг доменів", "🔗 Посилання"])

    # --- TAB 1: АНАЛІЗ ОХОПЛЕННЯ ---
    with tab1:
        st.markdown("#### 📊 Аналіз охоплення офіційних ресурсів")
        
        if not df_master.empty:
            total_rows = len(df_master)
            off_rows = df_master[df_master['is_official_dynamic'] == True]
            ext_rows = df_master[df_master['is_official_dynamic'] == False]
            
            def get_counts(df_sub):
                cnt = len(df_sub)
                if cnt == 0: return 0, 0, 0, 0
                p_c = len(df_sub[df_sub['provider'] == 'Perplexity'])
                g_c = len(df_sub[df_sub['provider'] == 'OpenAI GPT'])
                gem_c = len(df_sub[df_sub['provider'] == 'Google Gemini'])
                return cnt, p_c, g_c, gem_c

            tot_all, tot_p, tot_g, tot_gem = get_counts(df_master)
            off_all, off_p, off_g, off_gem = get_counts(off_rows)
            
            c_chart, c_stats = st.columns([2.5, 1.5], vertical_alignment="center")
            
            with c_chart:
                if total_rows > 0:
                    fig = px.pie(
                        names=["Офіційні", "Зовнішні"], 
                        values=[off_all, len(ext_rows)],
                        hole=0.55, 
                        color_discrete_sequence=["#00C896", "#E0E0E0"]
                    )
                    fig.update_layout(margin=dict(t=10, b=10, l=10, r=10), height=350, showlegend=True)
                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig, use_container_width=True, key="unique_chart_key_sources_1")
                else:
                    st.info("Немає даних.")

            with c_stats:
                st.markdown(f"""
                <div style="margin-bottom: 20px; padding:20px; border:1px solid #eee; border-radius:12px; background:white; box-shadow: 0 2px 5px rgba(0,0,0,0.05);">
                    <div style="color:#888; font-size:13px; font-weight:700; text-transform:uppercase; margin-bottom:5px;">Всього посилань</div>
                    <div style="font-size:32px; font-weight:800; color:#333; line-height:1;">{tot_all}</div>
                    <div style="margin-top:10px; font-size:12px; color:#555; display:flex; flex-direction:column; gap:3px;">
                        <div>🔹 Perplexity: <b>{tot_p}</b></div>
                        <div>🔸 OpenAI GPT: <b>{tot_g}</b></div>
                        <div>✨ Google Gemini: <b>{tot_gem}</b></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                st.markdown(f"""
                <div style="padding:20px; border:1px solid #00C896; border-radius:12px; background:#f0fdf9; box-shadow: 0 2px 5px rgba(0,200,150,0.1);">
                    <div style="color:#007a5c; font-size:13px; font-weight:700; text-transform:uppercase; margin-bottom:5px;">З них офіційні</div>
                    <div style="font-size:32px; font-weight:800; color:#00C896; line-height:1;">{off_all}</div>
                    <div style="margin-top:10px; font-size:12px; color:#005c45; display:flex; flex-direction:column; gap:3px;">
                        <div>🔹 Perplexity: <b>{off_p}</b></div>
                        <div>🔸 OpenAI GPT: <b>{off_g}</b></div>
                        <div>✨ Google Gemini: <b>{off_gem}</b></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

        else:
            st.info("Дані сканування відсутні.")

        st.divider()

        # --- РЕДАКТОР WHITELIST (З ОНОВЛЕНИМ ДИЗАЙНОМ) ---
        st.subheader("⚙️ Керування списком (Whitelist)")
        
        if "edit_whitelist_mode" not in st.session_state:
            st.session_state["edit_whitelist_mode"] = False

        # Готуємо DataFrame
        if assets_list_dicts:
            df_assets = pd.DataFrame(assets_list_dicts)
        else:
            df_assets = pd.DataFrame(columns=["Домен", "Мітка"])

        # Рахуємо статистику (скільки разів цей домен зустрічався в скануванні)
        if not df_master.empty:
            def get_stat_whitelist(dom):
                matches = df_master[df_master['url'].astype(str).str.contains(dom.lower(), case=False, na=False)]
                return len(matches)
            
            df_assets['Згадок'] = df_assets['Домен'].apply(get_stat_whitelist)
        else:
            df_assets['Згадок'] = 0

        # --- ВІДОБРАЖЕННЯ ТАБЛИЦІ (View Mode) ---
        if not st.session_state["edit_whitelist_mode"]:
            st.dataframe(
                df_assets,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Домен": st.column_config.TextColumn("Домен / URL", width="medium"),
                    "Мітка": st.column_config.TextColumn("Тип ресурсу", width="small"),
                    "Згадок": st.column_config.NumberColumn("Знайдено разів", format="%d")
                }
            )
            
            if st.button("✏️ Редагувати список"):
                st.session_state["edit_whitelist_mode"] = True
                st.rerun()
        
        # --- РЕЖИМ РЕДАГУВАННЯ (Edit Mode) ---
        else:
            st.info("Додайте або видаліть домени. Натисніть 'Зберегти' для застосування змін.")
            
            # Якщо таблиця пуста, додаємо рядок
            if df_assets.empty: 
                edit_df_input = pd.DataFrame([{"Домен": "", "Мітка": "Веб-сайт"}])
            else:
                edit_df_input = df_assets[["Домен", "Мітка"]]
            
            edited_df = st.data_editor(
                edit_df_input,
                num_rows="dynamic",
                use_container_width=True,
                hide_index=True, # Чисто, як просили
                column_config={
                    "Домен": st.column_config.TextColumn("Домен / URL", required=True),
                    "Мітка": st.column_config.SelectboxColumn(
                        "Тип ресурсу",
                        options=["Веб-сайт", "Соціальні мережі", "Стаття", "Інше"],
                        required=True
                    )
                }
            )
            
            c1, c2 = st.columns([1, 4])
            with c1:
                if st.button("💾 Зберегти", type="primary"):
                    try:
                        # 1. Видаляємо старі записи для цього проекту
                        supabase.table("official_assets").delete().eq("project_id", proj["id"]).execute()
                        
                        # 2. Формуємо нові
                        insert_data = []
                        for _, row in edited_df.iterrows():
                            d_val = str(row["Домен"]).strip()
                            if d_val:
                                insert_data.append({
                                    "project_id": proj["id"],
                                    "domain_or_url": d_val,
                                    "type": row["Мітка"]
                                })
                        
                        # 3. Вставляємо
                        if insert_data:
                            supabase.table("official_assets").insert(insert_data).execute()
                            
                        st.success("Список оновлено!")
                        st.session_state["edit_whitelist_mode"] = False
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Помилка збереження: {e}")
            with c2:
                if st.button("❌ Скасувати"):
                    st.session_state["edit_whitelist_mode"] = False
                    st.rerun()

    # --- TAB 2: РЕНКІНГ ---
    with tab2:
        st.markdown("#### 🏆 Ренкінг доменів")
        if not df_master.empty:
            all_kws = sorted(df_master['keyword_text'].unique())
            sel_kws_rank = st.multiselect("🔍 Фільтр по запитах:", all_kws, key="rank_kw_filter")
            
            df_rank_view = df_master.copy()
            if sel_kws_rank:
                df_rank_view = df_rank_view[df_rank_view['keyword_text'].isin(sel_kws_rank)]
            
            if not df_rank_view.empty:
                pivot_df = df_rank_view.pivot_table(
                    index='domain', columns='provider', values='mention_count', aggfunc='sum', fill_value=0
                ).reset_index()
                
                pivot_df['Всього'] = pivot_df.sum(axis=1, numeric_only=True)
                for col in ["Perplexity", "OpenAI GPT", "Google Gemini"]:
                    if col not in pivot_df.columns: pivot_df[col] = 0
                
                def get_meta(dom):
                    is_off = "Зовнішній"
                    for od in OFFICIAL_DOMAINS:
                        if od in dom.lower():
                            is_off = "Офіційний"
                            break
                    dates = df_rank_view[df_rank_view['domain'] == dom]['scan_date']
                    first = dates.min() if not dates.empty else None
                    first_str = pd.to_datetime(first).strftime("%Y-%m-%d") if first else "-"
                    return is_off, first_str

                pivot_df[['Тип', 'Вперше знайдено']] = pivot_df['domain'].apply(lambda x: pd.Series(get_meta(x)))
                pivot_df = pivot_df.sort_values("Всього", ascending=False).reset_index(drop=True)
                
                cols_order = ["domain", "Тип", "Всього", "Perplexity", "OpenAI GPT", "Google Gemini", "Вперше знайдено"]
                final_cols = [c for c in cols_order if c in pivot_df.columns]
                
                st.dataframe(
                    pivot_df[final_cols],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "domain": "Домен",
                        "Всього": st.column_config.NumberColumn(format="%d"),
                        "Perplexity": st.column_config.NumberColumn(format="%d"),
                        "OpenAI GPT": st.column_config.NumberColumn(format="%d"),
                        "Google Gemini": st.column_config.NumberColumn(format="%d"),
                    }
                )
            else:
                st.warning("Даних немає.")
        else:
            st.info("Дані відсутні.")

    # --- TAB 3: ПОСИЛАННЯ ---
    with tab3:
        st.markdown("#### 🔗 Детальний список посилань")
        if not df_master.empty:
            c_f1, c_f2 = st.columns([1, 1])
            with c_f1: sel_kws_links = st.multiselect("🔍 Фільтр по запитах:", all_kws, key="links_kw_filter")
            with c_f2: search_url = st.text_input("🔎 Пошук URL:", key="links_search")
            
            c_f3, c_f4 = st.columns(2)
            with c_f3: type_filter = st.selectbox("Тип ресурсу:", ["Всі", "Офіційні", "Зовнішні"], key="links_type_filter")
            
            df_links_view = df_master.copy()
            if sel_kws_links: df_links_view = df_links_view[df_links_view['keyword_text'].isin(sel_kws_links)]
            if search_url: df_links_view = df_links_view[df_links_view['url'].astype(str).str.contains(search_url, case=False)]
            if type_filter == "Офіційні": df_links_view = df_links_view[df_links_view['is_official_dynamic'] == True]
            elif type_filter == "Зовнішні": df_links_view = df_links_view[df_links_view['is_official_dynamic'] == False]

            if not df_links_view.empty:
                pivot_links = df_links_view.pivot_table(
                    index=['url', 'domain', 'is_official_dynamic'],
                    columns='provider', values='mention_count', aggfunc='sum', fill_value=0
                ).reset_index()
                
                pivot_links['Всього'] = pivot_links.sum(axis=1, numeric_only=True)
                for col in ["Perplexity", "OpenAI GPT", "Google Gemini"]:
                    if col not in pivot_links.columns: pivot_links[col] = 0
                
                pivot_links['Тип'] = pivot_links['is_official_dynamic'].apply(lambda x: "Офіційні" if x else "Зовнішні")
                pivot_links = pivot_links.sort_values("Всього", ascending=False).reset_index(drop=True)
                
                cols_order = ["url", "domain", "Тип", "Всього", "Perplexity", "OpenAI GPT", "Google Gemini"]
                final_cols = [c for c in cols_order if c in pivot_links.columns]
                
                st.dataframe(
                    pivot_links[final_cols],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "url": st.column_config.LinkColumn("Посилання", width="large"),
                        "Всього": st.column_config.NumberColumn(format="%d"),
                        "Perplexity": st.column_config.NumberColumn(format="%d"),
                        "OpenAI GPT": st.column_config.NumberColumn(format="%d"),
                        "Google Gemini": st.column_config.NumberColumn(format="%d"),
                    }
                )
            else:
                st.warning("Нічого не знайдено.")
        else:
            st.info("Дані відсутні.")


def show_history_page():
    """
    Сторінка історії сканувань.
    ВЕРСІЯ: PRETTY LLM NAMES.
    1. Перейменовує gpt-4o -> OpenAI, gemini -> Gemini тощо.
    2. Виправлено всі попередні помилки (Timezone, Merge).
    """
    import pandas as pd
    import streamlit as st
    from datetime import datetime, timedelta, timezone 

    # --- 1. ПІДКЛЮЧЕННЯ ---
    if 'supabase' in st.session_state:
        supabase = st.session_state['supabase']
    elif 'supabase' in globals():
        supabase = globals()['supabase']
    else:
        st.error("🚨 Помилка: Змінна 'supabase' не знайдена.")
        return

    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку оберіть проект.")
        return

    st.title("📜 Історія сканувань")

    # --- 2. ОТРИМАННЯ ДАНИХ ---
    with st.spinner("Завантаження історії..."):
        try:
            # 1. Keywords
            kw_resp = supabase.table("keywords").select("id, keyword_text").eq("project_id", proj["id"]).execute()
            kw_map = {k['id']: k['keyword_text'] for k in kw_resp.data} if kw_resp.data else {}

            # 2. Scans
            scans_resp = supabase.table("scan_results")\
                .select("id, created_at, provider, keyword_id")\
                .eq("project_id", proj["id"])\
                .order("created_at", desc=True)\
                .limit(500)\
                .execute()
            
            scans_data = scans_resp.data if scans_resp.data else []
            
            if not scans_data:
                st.info("Історія сканувань порожня.")
                return

            scan_ids = [s['id'] for s in scans_data]

            # 3. Mentions
            m_resp = supabase.table("brand_mentions")\
                .select("scan_result_id, is_my_brand, mention_count")\
                .in_("scan_result_id", scan_ids)\
                .execute()
            mentions_df = pd.DataFrame(m_resp.data) if m_resp.data else pd.DataFrame()

            # 4. Sources
            s_resp = supabase.table("extracted_sources")\
                .select("scan_result_id, is_official")\
                .in_("scan_result_id", scan_ids)\
                .execute()
            sources_df = pd.DataFrame(s_resp.data) if s_resp.data else pd.DataFrame()

        except Exception as e:
            st.error(f"Помилка завантаження даних: {e}")
            return

    # --- 3. ОБРОБКА ДАНИХ ---
    df_scans = pd.DataFrame(scans_data)

    # 🔥 МАПІНГ НАЗВ (Робимо це на початку)
    PROVIDER_MAP = {
        "gpt-4o": "OpenAI",
        "gpt-4-turbo": "OpenAI",
        "gemini-1.5-pro": "Gemini",
        "perplexity": "Perplexity"
    }
    # Замінюємо значення в колонці provider. Якщо значення немає в словнику, воно залишається як було.
    df_scans['provider'] = df_scans['provider'].replace(PROVIDER_MAP)
    
    # Підготовка
    df_scans['keyword'] = df_scans['keyword_id'].map(kw_map).fillna("Видалений запит")
    df_scans['created_at_dt'] = pd.to_datetime(df_scans['created_at'])
    
    # Агрегація Mentions
    if not mentions_df.empty:
        brands_count = mentions_df.groupby('scan_result_id').size().reset_index(name='total_brands')
        my_mentions = mentions_df[mentions_df['is_my_brand'] == True].groupby('scan_result_id')['mention_count'].sum().reset_index(name='my_mentions_count')
        
        df_scans = pd.merge(df_scans, brands_count, left_on='id', right_on='scan_result_id', how='left').fillna(0)
        if 'scan_result_id' in df_scans.columns: df_scans = df_scans.drop(columns=['scan_result_id'])
            
        df_scans = pd.merge(df_scans, my_mentions, left_on='id', right_on='scan_result_id', how='left').fillna(0)
        if 'scan_result_id' in df_scans.columns: df_scans = df_scans.drop(columns=['scan_result_id'])
    else:
        df_scans['total_brands'] = 0
        df_scans['my_mentions_count'] = 0

    # Агрегація Sources
    if not sources_df.empty:
        links_count = sources_df.groupby('scan_result_id').size().reset_index(name='total_links')
        official_count = sources_df[sources_df['is_official'] == True].groupby('scan_result_id').size().reset_index(name='official_links')
        
        df_scans = pd.merge(df_scans, links_count, left_on='id', right_on='scan_result_id', how='left').fillna(0)
        if 'scan_result_id' in df_scans.columns: df_scans = df_scans.drop(columns=['scan_result_id'])
            
        df_scans = pd.merge(
            df_scans, 
            official_count, 
            left_on='id', 
            right_on='scan_result_id', 
            how='left',
            suffixes=('', '_dup')
        ).fillna(0)
        
        if 'scan_result_id' in df_scans.columns: df_scans = df_scans.drop(columns=['scan_result_id'])
    else:
        df_scans['total_links'] = 0
        df_scans['official_links'] = 0

    # --- 4. ФІЛЬТРИ ТА СОРТУВАННЯ ---
    st.markdown("### 🔍 Фільтрація")
    
    c1, c2, c3 = st.columns([1, 1, 1.5])
    
    with c1:
        # Тепер тут будуть красиві назви (OpenAI, Gemini...)
        all_providers = df_scans['provider'].unique().tolist()
        sel_providers = st.multiselect("Модель (LLM)", all_providers, default=all_providers)
    
    with c2:
        date_options = ["Весь час", "Сьогодні", "Останні 7 днів", "Останні 30 днів"]
        sel_date = st.selectbox("Період", date_options)
        
    with c3:
        sort_opts = [
            "Найновіші спочатку", 
            "Найстаріші спочатку", 
            "Найбільше згадок бренду", 
            "Найменше згадок бренду",
            "Найбільше офіційних джерел",
            "Найбільше знайдених брендів"
        ]
        sel_sort = st.selectbox("Сортування", sort_opts)

    # Фільтрація
    mask = df_scans['provider'].isin(sel_providers)
    
    now = datetime.now(timezone.utc)
    
    if sel_date == "Сьогодні":
        mask &= (df_scans['created_at_dt'].dt.date == now.date())
    elif sel_date == "Останні 7 днів":
        mask &= (df_scans['created_at_dt'] >= (now - timedelta(days=7)))
    elif sel_date == "Останні 30 днів":
        mask &= (df_scans['created_at_dt'] >= (now - timedelta(days=30)))
        
    df_final = df_scans[mask].copy()

    # Сортування
    if sel_sort == "Найновіші спочатку":
        df_final = df_final.sort_values('created_at_dt', ascending=False)
    elif sel_sort == "Найстаріші спочатку":
        df_final = df_final.sort_values('created_at_dt', ascending=True)
    elif sel_sort == "Найбільше згадок бренду":
        df_final = df_final.sort_values('my_mentions_count', ascending=False)
    elif sel_sort == "Найменше згадок бренду":
        df_final = df_final.sort_values('my_mentions_count', ascending=True)
    elif sel_sort == "Найбільше офіційних джерел":
        df_final = df_final.sort_values('official_links', ascending=False)
    elif sel_sort == "Найбільше знайдених брендів":
        df_final = df_final.sort_values('total_brands', ascending=False)

    # --- 5. ВІДОБРАЖЕННЯ ---
    st.divider()
    st.markdown(f"**Знайдено записів:** {len(df_final)}")
    
    cols_to_show = [
        'created_at_dt', 'keyword', 'provider', 
        'total_brands', 'total_links', 'my_mentions_count', 'official_links'
    ]
    # Захист, якщо якихось колонок немає
    cols_to_show = [c for c in cols_to_show if c in df_final.columns]
    
    df_display = df_final[cols_to_show].copy()
    
    if 'created_at_dt' in df_display.columns:
        df_display['created_at_dt'] = df_display['created_at_dt'].dt.strftime('%d.%m.%Y %H:%M')

    st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "created_at_dt": "Дата та Час",
            "keyword": st.column_config.TextColumn("Запит", width="medium"),
            "provider": "LLM",
            "total_brands": st.column_config.NumberColumn("Всього брендів", help="Унікальних брендів"),
            "total_links": st.column_config.NumberColumn("Всього посилань", help="Всього знайдено"),
            "my_mentions_count": st.column_config.NumberColumn("Згадок нас", help="Наш бренд"),
            "official_links": st.column_config.NumberColumn("Офіц. джерела", help="Whitelist")
        }
    )


def sidebar_menu():
    """
    Бокове меню навігації.
    ВЕРСІЯ: FIXED & FULL (Menu, User Profile, Support, Navigation).
    """
    from streamlit_option_menu import option_menu
    import streamlit as st
    
    # Отримуємо дані з сесії
    proj = st.session_state.get("current_project")
    user = st.session_state.get("user")
    
    # Дані для відображення
    user_email = user.email if user else "Користувач"
    proj_name = proj.get("brand_name", "No Project") if proj else "Оберіть проект"
    proj_id = proj.get("id", "") if proj else ""

    with st.sidebar:
        # 1. Логотип
        st.image("https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png", width=160)
        
        st.divider()

        # 2. Профіль користувача (Відновлено)
        with st.container():
            c1, c2 = st.columns([0.2, 0.8])
            with c1:
                st.markdown("👤") # Або іконка аватара
            with c2:
                st.caption("Ви увійшли як:")
                st.markdown(f"**{user_email}**")
        
        st.write("") # Відступ

        # 3. Вибір проекту
        with st.expander(f"📁 {proj_name}", expanded=False):
            st.caption(f"ID: {proj_id}")
            if st.button("🔄 Змінити проект"):
                st.session_state["current_project"] = None
                st.rerun()

        st.write("") 

        # 4. Навігаційне меню
        options = [
            "Дашборд", 
            "Перелік запитів", 
            "Джерела", 
            "Конкуренти", 
            "Рекомендації", 
            "Історія сканувань", 
            "Звіти",             
            "FAQ",               
            "GPT-Visibility"
        ]
        
        icons = [
            "speedometer2", 
            "list-task", 
            "router", 
            "people", 
            "lightbulb", 
            "clock-history", 
            "file-earmark-text", 
            "question-circle",   
            "robot"
        ]

        # Додаємо Адмінку тільки для адмінів
        if st.session_state.get("role") in ["admin", "super_admin"]:
            options.append("Адмін")
            icons.append("shield-lock")

        selected = option_menu(
            "Меню",
            options,
            icons=icons,
            menu_icon="cast",
            default_index=0,
            styles={
                "container": {"padding": "0!important", "background-color": "transparent"},
                "icon": {"color": "grey", "font-size": "16px"}, 
                "nav-link": {"font-size": "14px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
                "nav-link-selected": {"background-color": "#00C896"},
            }
        )
        
        st.divider()

        # 5. Сапорт (Відновлено)
        st.caption("Потрібна допомога?")
        st.markdown("📧 **hi@virshi.ai**")

        # 6. Статус та Вихід
        if proj:
            st.write("")
            status = proj.get("status", "trial").upper()
            color = "orange" if status == "TRIAL" else "green" if status == "ACTIVE" else "red"
            st.markdown(f"Статус: **:{color}[{status}]**")
            
            if st.session_state.get("is_impersonating"):
                st.info("🕵️ Admin Mode")

        st.write("")
        if st.button("🚪 Вийти з акаунту", use_container_width=True):
            # Тут викликаємо вашу функцію logout
            if 'logout' in globals():
                logout()
            else:
                # Fallback, якщо функція logout не знайдена
                st.session_state.clear()
                st.rerun()

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
    Адмін-панель (CRM).
    ВЕРСІЯ: FINAL FIXES (RESET FIELDS, IMPORT URL, STATUS ERROR HANDLING).
    1. Tab 2: Виправлено очищення полів через динамічні ключі (fix 'cannot be modified').
    2. Tab 2: Додано імпорт запитів через URL.
    3. Tab 1: Обробка помилки ENUM для статусу 'blocked'.
    4. Tab 3: Проекти з нового рядка.
    """
    import pandas as pd
    import streamlit as st
    import numpy as np
    import requests
    import json
    import time
    import plotly.express as px
    import io
    import re

    # --- КОНСТАНТИ ---
    N8N_GEN_URL = "https://virshi.app.n8n.cloud/webhook/webhook/generate-prompts"

    # --- 0. ПІДКЛЮЧЕННЯ ---
    if 'supabase' not in globals():
        if 'supabase' in st.session_state:
            supabase = st.session_state['supabase']
        else:
            st.error("🚨 Помилка: Змінна 'supabase' не знайдена.")
            return
    else:
        supabase = globals()['supabase']

    # --- CSS ---
    st.markdown("""
    <style>
        .green-number { 
            background-color: #00C896; 
            color: white; 
            width: 24px; 
            height: 24px; 
            border-radius: 50%; 
            display: flex; 
            align-items: center; 
            justify-content: center; 
            font-weight: bold; 
            font-size: 12px; 
        }
        .del-kw-btn { color: #FF4B4B; cursor: pointer; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

    # --- STATE ДЛЯ ОЧИЩЕННЯ ФОРМИ ---
    if "admin_reset_id" not in st.session_state:
        st.session_state["admin_reset_id"] = 0

    # --- ХЕЛПЕРИ ---
    def clean_data_for_json(data):
        if isinstance(data, dict): return {k: clean_data_for_json(v) for k, v in data.items()}
        elif isinstance(data, list): return [clean_data_for_json(v) for v in data]
        elif isinstance(data, (np.int64, np.int32, np.integer)): return int(data)
        elif isinstance(data, (np.float64, np.float32, np.floating)): return float(data)
        elif isinstance(data, (np.bool_, bool)): return bool(data)
        elif pd.isna(data): return None
        return data

    def update_project_field(proj_id, field, value):
        try:
            val = clean_data_for_json(value)
            supabase.table("projects").update({field: val}).eq("id", proj_id).execute()
            
            # Очистка кешу
            if "my_projects" in st.session_state: del st.session_state["my_projects"]
            if "all_projects_admin" in st.session_state: del st.session_state["all_projects_admin"]
            
            # Оновлення поточного
            if "current_project" in st.session_state and st.session_state["current_project"]:
                if st.session_state["current_project"]["id"] == proj_id:
                    st.session_state["current_project"][field] = val
                
            st.toast(f"✅ Оновлено: {field} -> {value}")
            time.sleep(0.5)
        except Exception as e:
            err_msg = str(e)
            if "invalid input value for enum" in err_msg:
                st.error(f"⚠️ Помилка БД: Статус '{value}' не додано в ENUM (тип даних) у Supabase. Зверніться до розробника БД.")
            else:
                st.error(f"Помилка оновлення: {err_msg}")

    # --- ВЕБХУК ---
    def trigger_keyword_generation(brand, domain, industry, products):
        payload = { "brand": brand, "domain": domain, "industry": industry, "products": products }
        headers = {"virshi-auth": "hi@virshi.ai2025"}
        try:
            response = requests.post(N8N_GEN_URL, json=payload, headers=headers, timeout=60)
            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, dict):
                        if "prompts" in data: return data["prompts"]
                        if "keywords" in data: return data["keywords"]
                        return list(data.values()) if data else []
                    elif isinstance(data, list):
                        return data
                    return []
                except ValueError:
                    return []
            else:
                st.error(f"Error: {response.status_code}")
                return []
        except Exception as e:
            st.error(f"Connection error: {e}")
            return []

    # Ініціалізація списку
    if "new_proj_keywords" not in st.session_state:
        st.session_state["new_proj_keywords"] = [] 

    st.title("🛡️ Admin Panel (CRM)")

    # --- 1. ОТРИМАННЯ ДАНИХ ---
    try:
        projects_resp = supabase.table("projects").select("*").execute()
        projects_data = projects_resp.data if projects_resp.data else []

        kws_resp = supabase.table("keywords").select("project_id").execute()
        kws_df = pd.DataFrame(kws_resp.data) if kws_resp.data else pd.DataFrame()
        kw_counts = kws_df['project_id'].value_counts().to_dict() if not kws_df.empty else {}

        users_resp = supabase.table("profiles").select("*").execute()
        users_data = users_resp.data if users_resp.data else []
        
        user_map = {}
        for u in users_data:
            f_name = u.get('first_name', '') or ''
            l_name = u.get('last_name', '') or ''
            full_name = f"{f_name} {l_name}".strip() or u.get('email', 'Unknown')
            user_map[u['id']] = {
                "full_name": full_name,
                "role": u.get('role', 'user'),
                "email": u.get('email', '-'),
                "created_at": u.get('created_at', '')
            }

    except Exception as e:
        st.error(f"Помилка завантаження даних: {e}")
        return

    # --- 2. KPI ---
    if projects_data:
        df_stats = pd.DataFrame(projects_data)
        total = len(df_stats)
        active = len(df_stats[df_stats['status'] == 'active'])
        blocked = len(df_stats[df_stats['status'] == 'blocked'])
        trial = len(df_stats[df_stats['status'] == 'trial'])
        
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Всього проектів", total)
        k2.metric("Active", active)
        k3.metric("Trial", trial)
        k4.metric("Blocked", blocked)

    st.write("")

    # --- 3. ВКЛАДКИ ---
    tab_list, tab_create, tab_users = st.tabs(["📂 Список проектів", "➕ Створити проект", "👥 Користувачі & Права"])

    # ========================================================
    # TAB 1: СПИСОК ПРОЕКТІВ
    # ========================================================
    with tab_list:
        st.markdown("##### 🔍 Фільтрація та Пошук")
        
        fc1, fc2, fc3 = st.columns([2, 1.5, 1])
        with fc1:
            search_query = st.text_input("Пошук", placeholder="Назва, ID, домен, email власника", key="adm_search")
        with fc2:
            status_filter = st.multiselect("Статус", ["active", "trial", "blocked"], default=[], key="adm_status_filter", placeholder="Всі статуси")
        with fc3:
            sort_order = st.selectbox("Сортування", ["Найновіші", "Найстаріші"], key="adm_sort")

        st.divider()
        
        filtered_projects = []
        if projects_data:
            for p in projects_data:
                u_id = p.get('user_id')
                owner = user_map.get(u_id, {"full_name": "", "email": ""})
                
                p_name = p.get('brand_name') or p.get('project_name') or ""
                p_domain = p.get('domain') or ""
                p_id_str = str(p.get('id', ''))
                
                search_text = f"{p_name} {p_domain} {p_id_str} {owner['full_name']} {owner['email']}".lower()
                
                if search_query and search_query.lower() not in search_text: continue
                if status_filter and p.get('status', 'trial') not in status_filter: continue
                
                filtered_projects.append(p)

            reverse_sort = True if sort_order == "Найновіші" else False
            filtered_projects.sort(key=lambda x: x.get('created_at', ''), reverse=reverse_sort)

        # Header
        h0, h1, h_dash, h2, h3, h_cnt, h4, h5 = st.columns([0.3, 2.2, 0.5, 1.5, 1.2, 0.8, 1, 0.5])
        h0.markdown("**#**")
        h1.markdown("**Проект / Користувач**")
        h_dash.markdown("") 
        h2.markdown("**Статус**")
        h3.markdown("**Автосканування**")
        h_cnt.markdown("**Запитів**")
        h4.markdown("**Дата**")
        h5.markdown("**Дії**")
        st.markdown("<hr style='margin: 5px 0'>", unsafe_allow_html=True)

        if not filtered_projects: st.info("Проектів не знайдено.")

        for idx, p in enumerate(filtered_projects, 1):
            p_id = p['id']
            u_id = p.get('user_id')
            owner_info = user_map.get(u_id, {"full_name": "Невідомий", "role": "user", "email": "-"})
            
            raw_name = p.get('brand_name') or p.get('project_name')
            domain = p.get('domain', '')
            if raw_name:
                clean_name = str(raw_name).replace('*', '').strip()
            else:
                clean_name = domain.replace('https://', '').replace('www.', '').split('/')[0] if domain else "Без назви"

            k_count = kw_counts.get(p_id, 0)

            with st.container():
                c0, c1, c_dash, c2, c3, c_cnt, c4, c5 = st.columns([0.3, 2.2, 0.5, 1.5, 1.2, 0.8, 1, 0.5])

                with c0: st.caption(f"{idx}")

                with c1:
                    st.markdown(f"**{clean_name}**")
                    st.caption(f"ID: `{p_id}`")
                    if domain: st.caption(f"🌐 {domain}")
                    st.caption(f"👤 {owner_info['full_name']} | {owner_info['email']}")

                with c_dash:
                    if st.button("↗️", key=f"goto_{p_id}", help="Відкрити дашборд"):
                        st.session_state["current_project"] = p
                        st.session_state["selected_page"] = "Дашборд"
                        st.session_state["focus_keyword_id"] = None
                        st.rerun()

                with c2:
                    curr_status = p.get('status', 'trial')
                    opts = ["trial", "active", "blocked"]
                    try: idx_s = opts.index(curr_status)
                    except: idx_s = 0
                    
                    new_status = st.selectbox("St", opts, index=idx_s, key=f"st_{p_id}", label_visibility="collapsed")
                    if new_status != curr_status:
                        update_project_field(p_id, "status", new_status)

                with c3:
                    allow_cron = p.get('allow_cron', False)
                    new_cron = st.checkbox("Дозволити", value=allow_cron, key=f"cr_{p_id}")
                    if new_cron != allow_cron:
                        update_project_field(p_id, "allow_cron", new_cron)

                with c_cnt:
                    st.markdown(f"**{k_count}**")

                with c4:
                    raw_date = p.get('created_at', '')
                    if raw_date: st.caption(raw_date[:10])

                with c5:
                    confirm_key = f"confirm_del_{p_id}"
                    if not st.session_state.get(confirm_key, False):
                        if st.button("🗑", key=f"del_btn_{p_id}"):
                            st.session_state[confirm_key] = True
                            st.rerun()
                    else:
                        if st.button("✅", key=f"yes_{p_id}"):
                            try:
                                supabase.table("projects").delete().eq("id", p_id).execute()
                                st.success("Видалено!")
                                time.sleep(0.5)
                                st.rerun()
                            except Exception as e:
                                st.error(str(e))
                        if st.button("❌", key=f"no_{p_id}"):
                            st.session_state[confirm_key] = False
                            st.rerun()
                
                st.markdown("<hr style='margin: 5px 0; border-top: 1px solid #eee;'>", unsafe_allow_html=True)

    # ========================================================
    # TAB 2: СТВОРИТИ ПРОЕКТ
    # ========================================================
    with tab_create:
        st.markdown("##### Створення нового проекту")
        
        # Використовуємо динамічний ключ для скидання полів
        rk = st.session_state["admin_reset_id"]
        
        c1, c2 = st.columns(2)
        new_name_val = c1.text_input("Назва проекту (Бренд)", key=f"new_proj_name_{rk}", placeholder="Наприклад: SkyUp")
        new_domain_val = c2.text_input("Домен", key=f"new_proj_domain_{rk}", placeholder="skyup.aero")
        
        c3, c4 = st.columns(2)
        new_industry_val = c3.text_input("Галузь (Обов'язково)", key=f"new_proj_ind_{rk}", placeholder="напр. авіаперевезення")
        
        region_options = ["Ukraine", "USA", "Europe", "Global"]
        new_region_val = c4.selectbox("Регіон", region_options, key=f"new_proj_region_{rk}")

        new_desc_val = st.text_area("Продукти/Послуги", placeholder="напр. лоукостер, квитки", height=68, key=f"new_proj_desc_{rk}")
        
        if st.button("✨ Згенерувати 10 запитів (AI)", key=f"btn_gen_{rk}"):
            if new_domain_val and new_industry_val and new_desc_val: 
                brand_for_ai = new_name_val if new_name_val else new_domain_val.split('.')[0]
                
                with st.spinner("Звертаємось до n8n для генерації..."):
                    generated_kws = trigger_keyword_generation(
                        brand=brand_for_ai,
                        domain=new_domain_val,
                        industry=new_industry_val,
                        products=new_desc_val
                    )
                
                if generated_kws:
                    current_kws = st.session_state["new_proj_keywords"]
                    for kw in generated_kws:
                        current_kws.append({"keyword": kw})
                    st.session_state["new_proj_keywords"] = current_kws
                    st.success(f"Додано {len(generated_kws)} запитів!")
                else:
                    st.warning("Вебхук не повернув даних.")
            else:
                st.warning("⚠️ Заповніть: Домен, Галузь та Продукти.")

        st.divider()
        st.markdown("###### 📝 Редагування запитів перед створенням")
        
        # --- ІМПОРТ (FILE & URL) ---
        with st.expander("📥 Імпорт (Excel / URL)", expanded=False):
            st.info("💡 Завантажте файл .xlsx або вставте посилання на Google Sheet. Перша колонка має називатися **Keyword**.")
            
            import_source = st.radio("Джерело:", ["Файл (.xlsx)", "Посилання (URL)"], horizontal=True, key=f"admin_imp_src_{rk}")
            df_upload = None
            
            if import_source == "Файл (.xlsx)":
                uploaded_file = st.file_uploader("Оберіть файл Excel", type=["xlsx"], key=f"admin_kw_import_file_{rk}")
                if uploaded_file:
                    try:
                        df_upload = pd.read_excel(uploaded_file)
                    except Exception as e:
                        st.error(f"Помилка файлу: {e}")
            else:
                import_url = st.text_input("Вставте посилання (Google Sheets або CSV):", key=f"admin_kw_import_url_{rk}")
                if import_url:
                    try:
                        if "docs.google.com" in import_url:
                            match = re.search(r'/d/([a-zA-Z0-9-_]+)', import_url)
                            if match:
                                sheet_id = match.group(1)
                                csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
                                df_upload = pd.read_csv(csv_url)
                            else:
                                st.error("Не вдалося розпізнати ID Google Sheet.")
                        elif import_url.endswith(".csv"):
                            df_upload = pd.read_csv(import_url)
                        elif import_url.endswith(".xlsx"):
                            df_upload = pd.read_excel(import_url)
                        else:
                            st.warning("Пробуємо як CSV...")
                            df_upload = pd.read_csv(import_url)
                    except Exception as e:
                        if "400" in str(e): st.error("Помилка 400. Перевірте доступ (Anyone with the link).")
                        else: st.error(f"Помилка URL: {e}")

            if df_upload is not None:
                target_col = None
                cols_lower = [str(c).lower().strip() for c in df_upload.columns]
                if "keyword" in cols_lower: target_col = df_upload.columns[cols_lower.index("keyword")]
                elif "запит" in cols_lower: target_col = df_upload.columns[cols_lower.index("запит")]
                else: target_col = df_upload.columns[0]
                
                imp_kws = df_upload[target_col].dropna().astype(str).tolist()
                
                if st.button(f"Додати {len(imp_kws)} запитів", key=f"btn_add_imp_{rk}"):
                    current_kws = st.session_state["new_proj_keywords"]
                    for kw in imp_kws:
                        current_kws.append({"keyword": kw})
                    st.session_state["new_proj_keywords"] = current_kws
                    st.success("Імпортовано!")
                    st.rerun()

        # --- ТАБЛИЦЯ ЗАПИТІВ ---
        keywords_list = st.session_state["new_proj_keywords"]
        
        if not keywords_list:
            st.info("Список запитів порожній. Додайте вручну або згенеруйте.")
        else:
            for i, item in enumerate(keywords_list):
                with st.container(border=True):
                    c_num, c_txt, c_act = st.columns([0.5, 8, 1])
                    with c_num:
                        st.markdown(f"<div class='green-number'>{i+1}</div>", unsafe_allow_html=True)
                    with c_txt:
                        new_val = st.text_input("kw", value=item['keyword'], key=f"edit_kw_adm_{i}_{rk}", label_visibility="collapsed")
                        if new_val != item['keyword']:
                            st.session_state["new_proj_keywords"][i]['keyword'] = new_val
                    with c_act:
                        if st.button("🗑️", key=f"del_kw_adm_{i}_{rk}"):
                            st.session_state["new_proj_keywords"].pop(i)
                            st.rerun()

        if st.button("➕ Додати рядок", key=f"btn_plus_{rk}"):
            st.session_state["new_proj_keywords"].append({"keyword": ""})
            st.rerun()

        st.divider()
        c_st, c_cr = st.columns(2)
        new_status = c_st.selectbox("Початковий статус", ["trial", "active", "blocked"], key=f"new_proj_status_{rk}")
        new_cron = c_cr.checkbox("Дозволити автосканування одразу?", value=False, key=f"new_proj_cron_{rk}")

        if st.button("🚀 Створити проект та зберегти запити", type="primary", key=f"btn_create_{rk}"):
            final_name = new_name_val if new_name_val else new_domain_val.split('.')[0].capitalize()
            
            if new_domain_val:
                try:
                    current_user_id = st.session_state["user"].id
                    
                    new_proj_data = {
                        "user_id": current_user_id,
                        "brand_name": final_name, 
                        "domain": new_domain_val,
                        "status": new_status,
                        "allow_cron": new_cron,
                        "region": new_region_val
                    }
                    res_proj = supabase.table("projects").insert(new_proj_data).execute()
                    
                    if res_proj.data:
                        new_proj_id = res_proj.data[0]['id']
                        
                        # Whitelist Clean
                        try:
                            clean_d = new_domain_val.replace("https://", "").replace("http://", "").replace("www.", "").strip().rstrip("/")
                            supabase.table("official_assets").insert({
                                "project_id": new_proj_id, 
                                "domain_or_url": clean_d, 
                                "type": "website"
                            }).execute()
                        except: pass

                        final_kws_clean = [k['keyword'].strip() for k in keywords_list if k['keyword'].strip()]
                        
                        if final_kws_clean:
                            kws_data = [
                                {
                                    "project_id": new_proj_id, 
                                    "keyword_text": kw,
                                    "is_active": True
                                } for kw in final_kws_clean
                            ]
                            supabase.table("keywords").insert(kws_data).execute()
                        
                        # --- SUCCESS & RESET ---
                        st.session_state["new_proj_keywords"] = [] 
                        if "my_projects" in st.session_state: del st.session_state["my_projects"]
                        
                        # Змінюємо ключ, щоб очистити інпути
                        st.session_state["admin_reset_id"] += 1
                        
                        st.success(f"✅ Проект '{final_name}' успішно створено!")
                        time.sleep(2)
                        st.rerun()
                except Exception as e:
                    st.error(f"Помилка створення: {e}")
            else:
                st.warning("Домен обов'язковий.")

    # ========================================================
    # TAB 3: КОРИСТУВАЧІ ТА ПРАВА (NEW LINE PROJECTS)
    # ========================================================
    with tab_users:
        st.markdown("##### 👥 База користувачів")

        uf1, uf2 = st.columns(2)
        with uf1:
            u_search = st.text_input("🔍 Пошук користувача", placeholder="Ім'я або email")
        with uf2:
            role_filter = st.multiselect("Роль", ["user", "admin", "super_admin"], default=[])

        if users_data:
            proj_df = pd.DataFrame(projects_data)
            
            user_table_data = []
            for u in users_data:
                full_name = f"{u.get('first_name', '')} {u.get('last_name', '')}".strip()
                email = u.get('email', '')
                
                search_target = f"{full_name} {email}".lower()
                if u_search and u_search.lower() not in search_target: continue
                if role_filter and u.get('role', 'user') not in role_filter: continue

                user_projs = []
                if not proj_df.empty and 'user_id' in proj_df.columns:
                    my_projs = proj_df[proj_df['user_id'] == u['id']]
                    for _, p_row in my_projs.iterrows():
                        p_nm = p_row.get('brand_name') or p_row.get('project_name') or 'NoName'
                        p_dt = p_row.get('created_at', '')[:10]
                        user_projs.append(f"{p_nm} ({p_dt})")
                
                # 🔥 FIX: Новий рядок
                projs_str = "\n".join(user_projs) if user_projs else "-"

                user_table_data.append({
                    "id": u['id'],
                    "Ім'я": full_name,
                    "Email": email,
                    "Роль": u.get('role', 'user'),
                    "Проекти": projs_str, 
                    "Зареєстрований": u.get('created_at', '')[:10]
                })
            
            df_users_view = pd.DataFrame(user_table_data)
            
            if not df_users_view.empty:
                df_users_view.index = np.arange(1, len(df_users_view) + 1)
                
                edited_users = st.data_editor(
                    df_users_view,
                    column_config={
                        "id": st.column_config.TextColumn("User ID", disabled=True, width="small"),
                        "Email": st.column_config.TextColumn("Email", disabled=True),
                        "Ім'я": st.column_config.TextColumn("Ім'я", disabled=True),
                        "Проекти": st.column_config.TextColumn("Проекти (Дата)", disabled=True, width="large"),
                        "Зареєстрований": st.column_config.TextColumn("Дата реєстрації", disabled=True),
                        "Роль": st.column_config.SelectboxColumn("Роль", options=["user", "admin", "super_admin"], required=True)
                    },
                    use_container_width=True,
                    key="admin_users_final_v4"
                )

                if st.button("💾 Зберегти зміни прав"):
                    try:
                        changes_count = 0
                        updated_rows = edited_users.to_dict('index') 
                        
                        for idx, row in updated_rows.items():
                            uid = row['id']
                            new_role = row['Роль']
                            
                            old_user = next((u for u in users_data if u['id'] == uid), None)
                            if old_user and old_user.get('role') != new_role:
                                supabase.table("profiles").update({"role": new_role}).eq("id", uid).execute()
                                changes_count += 1
                        
                        if changes_count > 0:
                            st.success(f"Успішно оновлено {changes_count} користувачів!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.info("Змін не виявлено.")
                            
                    except Exception as e:
                        st.error(f"Помилка збереження: {e}")

                st.divider()
                st.markdown("##### 📈 Динаміка реєстрацій")
                
                df_chart = pd.DataFrame(users_data)
                if 'created_at' in df_chart.columns:
                    df_chart['date'] = pd.to_datetime(df_chart['created_at']).dt.date
                    from datetime import timedelta
                    time_filter = st.selectbox("Період", ["Останні 7 днів", "Останні 30 днів", "Останні 90 днів", "Весь час"], index=1)
                    
                    today = pd.to_datetime("today").date()
                    if "7" in time_filter: start_date = today - timedelta(days=7)
                    elif "30" in time_filter: start_date = today - timedelta(days=30)
                    elif "90" in time_filter: start_date = today - timedelta(days=90)
                    else: start_date = df_chart['date'].min()
                    
                    df_chart_filtered = df_chart[df_chart['date'] >= start_date]
                    reg_counts = df_chart_filtered.groupby('date').size().reset_index(name='count')
                    
                    if not reg_counts.empty:
                        fig = px.bar(reg_counts, x='date', y='count', labels={'date': 'Дата', 'count': 'Нових користувачів'})
                        fig.update_layout(height=300)
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("Немає реєстрацій за цей період.")
            else:
                st.warning("Користувачів не знайдено.")
        else:
            st.warning("База користувачів пуста.")


def show_chat_page():
    """
    Сторінка AI-асистента (GPT-Visibility).
    ВЕРСІЯ: ADDED CONTEXT (SOURCES, BRAND, USER NAME).
    1. Передає official_sources (список доменів з бази).
    2. Передає user_name (з метаданих або email).
    3. Передає target_brand.
    """
    import requests
    import streamlit as st

    # --- КОНФІГУРАЦІЯ ---
    # Перевірка наявності URL
    if 'N8N_CHAT_WEBHOOK' not in globals():
        target_url = st.secrets.get("N8N_CHAT_WEBHOOK", "")
        if not target_url:
            st.error("🚨 Не задано посилання N8N_CHAT_WEBHOOK.")
            return
    else:
        target_url = N8N_CHAT_WEBHOOK

    # Підключення до бази (для отримання джерел)
    if 'supabase' in st.session_state:
        supabase = st.session_state['supabase']
    elif 'supabase' in globals():
        supabase = globals()['supabase']
    else:
        st.error("🚨 Змінна 'supabase' не знайдена.")
        return

    headers = {
        "virshi-auth": "hi@virshi.ai2025" 
    }

    st.title("🤖 GPT-Visibility Assistant")
    
    # 1. Отримуємо контекст користувача та проекту
    user = st.session_state.get("user")
    role = st.session_state.get("role", "user") 
    proj = st.session_state.get("current_project", {})
    
    if not proj:
        st.warning("⚠️ Будь ласка, оберіть проект у меню зліва.")
        return

    # 2. Логіка отримання імені користувача
    user_name = "Користувач"
    if user:
        # Спроба дістати ім'я з метаданих Supabase, інакше email
        meta = getattr(user, "user_metadata", {})
        user_name = meta.get("full_name") or meta.get("name") or user.email.split("@")[0]

    # 3. Логіка отримання офіційних джерел (Whitelist)
    official_sources_list = []
    try:
        # Робимо запит до бази, щоб агент знав "білий список"
        assets_resp = supabase.table("official_assets")\
            .select("domain_or_url")\
            .eq("project_id", proj.get("id"))\
            .execute()
        
        if assets_resp.data:
            official_sources_list = [item["domain_or_url"] for item in assets_resp.data]
    except Exception:
        official_sources_list = [] # Якщо помилка, просто пустий список

    # 4. Ініціалізація історії
    if "messages" not in st.session_state:
        brand_name = proj.get('brand_name', 'вашого бренду')
        welcome_text = f"Привіт, {user_name}! Я аналітик проекту **{brand_name}**. Готовий допомогти з аналізом видимості та конкурентів."
        st.session_state["messages"] = [
            {"role": "assistant", "content": welcome_text}
        ]

    # 5. Відображення історії
    for msg in st.session_state["messages"]:
        avatar_icon = "👤" if msg["role"] == "user" else "🤖"
        with st.chat_message(msg["role"], avatar=avatar_icon):
            st.markdown(msg["content"])

    # 6. Обробка вводу
    if prompt := st.chat_input("Напишіть ваше запитання..."):
        
        # A. Користувач
        st.session_state["messages"].append({"role": "user", "content": prompt})
        with st.chat_message("user", avatar="👤"):
            st.markdown(prompt)

        # B. Відправка на n8n
        with st.chat_message("assistant", avatar="🤖"):
            message_placeholder = st.empty()
            
            with st.spinner("Аналізую дані..."):
                try:
                    # --- 🔥 РОЗШИРЕНИЙ PAYLOAD ---
                    payload = {
                        "query": prompt,
                        
                        # Користувач
                        "user_id": user.id if user else "guest",
                        "user_email": user.email if user else None,
                        "user_name": user_name,  # <--- Ім'я
                        "role": role,
                        
                        # Проект
                        "project_id": proj.get("id"),
                        "project_name": proj.get("brand_name"),
                        "target_brand": proj.get("brand_name"), # <--- Цільовий бренд
                        "domain": proj.get("domain"),
                        "status": proj.get("status"),
                        
                        # Контекст
                        "official_sources": official_sources_list # <--- Список джерел
                    }

                    response = requests.post(
                        target_url, 
                        json=payload, 
                        headers=headers, 
                        timeout=60
                    )

                    if response.status_code == 200:
                        data = response.json()
                        bot_reply = data.get("output") or data.get("answer") or data.get("text")
                        
                        if isinstance(bot_reply, dict):
                            bot_reply = str(bot_reply)
                        
                        if not bot_reply:
                            bot_reply = "⚠️ Отримана пуста відповідь від AI."
                            
                    elif response.status_code == 403:
                        bot_reply = "⛔ Помилка 403: Доступ заборонено. Перевірте Header Name 'virshi-auth' в n8n."
                    elif response.status_code == 404:
                        bot_reply = f"⚠️ Помилка 404 (Not Found).\n\n1. Перевірте метод **POST** в n8n.\n2. Перевірте, що Workflow **Active**."
                    else:
                        bot_reply = f"⚠️ Помилка сервера: {response.status_code} - {response.text}"

                except Exception as e:
                    bot_reply = f"⚠️ Помилка з'єднання: {e}"

                # C. Вивід
                message_placeholder.markdown(bot_reply)
        
        # D. Збереження
        st.session_state["messages"].append({"role": "assistant", "content": bot_reply})
        
            
def main():
    # 1. Session Check
    if 'check_session' in globals():
        check_session()

    # 2. If not logged in -> Show Auth Page
    if not st.session_state.get("user"):
        # Переконайтеся, що show_auth_page визначена
        if 'show_auth_page' in globals():
            show_auth_page()
        else:
            st.error("Функція авторизації не знайдена.")
        return

    # 3. ОТРИМАННЯ ДАНИХ ПРОЕКТУ
    if not st.session_state.get("current_project"):
        try:
            user_id = st.session_state["user"].id
            resp = supabase.table("projects").select("*").eq("user_id", user_id).execute()
            if resp.data:
                # Беремо перший знайдений проект
                st.session_state["current_project"] = resp.data[0]
                st.rerun()
        except Exception:
            pass

    # 4. ЛОГІКА ONBOARDING
    # Якщо проекту немає і це не адмін
    user_role = st.session_state.get("role", "user")
    
    if st.session_state.get("current_project") is None and user_role not in ["admin", "super_admin"]:
        with st.sidebar:
            # Логотип
            st.image("https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png", width=150)
            if st.button("Вийти"):
                logout()
        
        # Запуск майстра
        if 'onboarding_wizard' in globals():
            onboarding_wizard()
        else:
            st.error("Onboarding Wizard not found.")
    
    # 5. ОСНОВНИЙ ДОДАТОК
    else:
        # Виклик меню
        page = sidebar_menu()

        # Роутинг сторінок
        if page == "Дашборд":
            show_dashboard()
        elif page == "Перелік запитів":
            show_keywords_page()
        elif page == "Джерела":
            show_sources_page()
        elif page == "Конкуренти":
            # Якщо окремої сторінки немає, можна використати частину дашборду або заглушку
            if 'show_competitors_page' in globals():
                show_competitors_page()
            else:
                st.info("Розділ у розробці (див. Дашборд).")
        elif page == "Рекомендації":
            show_recommendations_page()
            
        # --- НОВІ СТОРІНКИ ---
        elif page == "Історія сканувань":
            if 'show_history_page' in globals(): show_history_page()
            else: st.warning("Функція show_history_page не знайдена.")
            
        elif page == "Звіти":
            if 'show_reports_page' in globals(): show_reports_page()
            else: st.warning("Функція show_reports_page не знайдена.")
            
        elif page == "FAQ":
            if 'show_faq_page' in globals(): show_faq_page()
            else: st.warning("Функція show_faq_page не знайдена.")
        # ---------------------

        elif page == "GPT-Visibility":
            show_chat_page()
            
        elif page == "Адмін":
            if user_role in ["admin", "super_admin"]:
                show_admin_page()
            else:
                st.error("Доступ заборонено.")

if __name__ == "__main__":
    main()
