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
import uuid


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
        response = requests.post(N8N_GEN_URL, json=payload, headers=headers, timeout=60)

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
    ВЕРСІЯ: TRIAL LOGIC UPDATE (One-time scan per keyword).
    1. Trial дозволяє сканувати будь-яку модель.
    2. Trial дозволяє сканувати конкретний запит лише 1 раз.
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

    # Переконайтеся, що URL вебхука доступний
    if 'N8N_ANALYZE_URL' not in globals():
        # Спробуйте взяти з secrets або захардкодити, якщо немає
        N8N_ANALYZE_URL = st.secrets.get("N8N_ANALYZE_URL", "https://virshi.app.n8n.cloud/webhook/webhook/analyze") 
    else:
        N8N_ANALYZE_URL = globals()['N8N_ANALYZE_URL']

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
        st.error("⛔ Проект заблоковано. Зверніться до адміністратора.")
        return False

    if not models:
        models = ["Perplexity"] # Default

    # Нормалізація keywords (завжди список)
    if isinstance(keywords, str):
        keywords_list = [keywords]
    else:
        keywords_list = keywords

    # ==========================================
    # 🔥 ЛОГІКА ТРІАЛУ (НОВА)
    # ==========================================
    if status == "trial":
        try:
            # Крок 1: Знаходимо ID для переданих ключових слів
            # Нам треба знати ID, щоб перевірити scan_results
            kw_resp = supabase.table("keywords")\
                .select("id, keyword_text")\
                .eq("project_id", project_id)\
                .in_("keyword_text", keywords_list)\
                .execute()
            
            kw_map = {item['keyword_text']: item['id'] for item in kw_resp.data} if kw_resp.data else {}
            
            allowed_keywords = []
            blocked_keywords = []

            for kw_text in keywords_list:
                kw_id = kw_map.get(kw_text)
                
                if kw_id:
                    # Крок 2: Перевіряємо, чи вже сканували цей ID
                    # Шукаємо хоча б один запис у scan_results для цього keyword_id
                    existing_scan = supabase.table("scan_results")\
                        .select("id", count="exact")\
                        .eq("keyword_id", kw_id)\
                        .limit(1)\
                        .execute()
                    
                    if existing_scan.count and existing_scan.count > 0:
                        blocked_keywords.append(kw_text)
                    else:
                        allowed_keywords.append(kw_text)
                else:
                    # Якщо ID не знайдено (наприклад, слово ще не записано в БД), 
                    # то теоретично це "нове" слово. 
                    # Але n8n зазвичай очікує, що слова вже є в базі.
                    # Дозволяємо, сподіваючись, що n8n розбереться або створить.
                    allowed_keywords.append(kw_text)

            if blocked_keywords:
                st.warning(f"🔒 Наступні запити вже були проскановані (Trial ліміт 1 раз): {', '.join(blocked_keywords[:3])}...")
            
            if not allowed_keywords:
                st.error("⛔ Всі обрані запити вже були проскановані. У статусі Trial повторне сканування заборонено.")
                return False
            
            # Оновлюємо список для відправки (лишаємо тільки дозволені)
            keywords_list = allowed_keywords

        except Exception as e:
            print(f"Trial check error: {e}")
            # У випадку помилки перевірки - краще пропустити або заблокувати?
            # Для безпеки можна заблокувати, але для UX краще показати warning
            st.warning("⚠️ Не вдалося перевірити ліміти Trial. Спробуйте пізніше.")
            return False

    try:
        user = st.session_state.get("user")
        user_email = user.email if user else "no-reply@virshi.ai"
        
        success_count = 0

        # --- 3. ОТРИМАННЯ ТА ЧИСТКА WHITELIST ---
        clean_assets = []
        try:
            assets_resp = supabase.table("official_assets")\
                .select("domain_or_url")\
                .eq("project_id", project_id)\
                .execute()
            
            if assets_resp.data:
                for item in assets_resp.data:
                    raw_url = item.get("domain_or_url", "").lower().strip()
                    clean = raw_url.replace("https://", "").replace("http://", "").replace("www.", "").rstrip("/")
                    if clean:
                        clean_assets.append(clean)
        except Exception as e:
            print(f"Error fetching assets: {e}")
            clean_assets = []

        headers = {"virshi-auth": "hi@virshi.ai2025"}

        # 4. ВІДПРАВКА
        # Важливо: Якщо Trial відфільтрував слова, відправляємо тільки allowed_keywords
        if not keywords_list:
             return False

        for ui_model_name in models:
            tech_model_id = MODEL_MAPPING.get(ui_model_name, ui_model_name)

            payload = {
                "project_id": project_id,
                "keywords": keywords_list, # Вже відфільтровані
                "brand_name": brand_name,
                "user_email": user_email,
                "provider": tech_model_id,
                "models": [tech_model_id],
                "official_assets": clean_assets 
            }
            
            try:
                response = requests.post(
                    N8N_ANALYZE_URL, 
                    json=payload, 
                    headers=headers, 
                    timeout=60
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
# 6. render_sidebar
# =========================
def render_sidebar():
    with st.sidebar:
        # --- ЛОГОТИП ---
        # Замініть на ваш реальний шлях до лого або URL
        st.image("https://raw.githubusercontent.com/virshi-ai/image/39ba460ec649893b9495427aa102420beb1fa48d/virshi-op_logo-main.png", width=150)
        
        st.markdown("---")
        
        # --- ІНФО ПРО КОРИСТУВАЧА ---
        user_email = st.session_state.get("user", {}).get("email", "User")
        user_role = st.session_state.get("role", "user")
        
        st.caption(f"Ви авторизовані як:\n**{user_role.capitalize()}**")
        st.caption(user_email)
        
        st.markdown("---")

        # --- ВИБІР ПРОЕКТУ (Критично важливо для меню) ---
        # Перевіряємо, чи завантажені проекти. Якщо ні - пробуємо завантажити.
        if "projects" not in st.session_state or not st.session_state["projects"]:
            try:
                # Тут ваш запит до Supabase
                response = st.session_state['supabase'].table("projects").select("*").execute()
                st.session_state["projects"] = response.data
            except:
                st.session_state["projects"] = []

        projects = st.session_state["projects"]
        
        # Якщо проектів немає
        if not projects:
            st.warning("Проекти не знайдені")
        else:
            # Знаходимо індекс поточного проекту
            project_names = [p['brand_name'] for p in projects]
            current_p = st.session_state.get("current_project", {})
            default_index = 0
            
            if current_p:
                try:
                    default_index = project_names.index(current_p['brand_name'])
                except ValueError:
                    default_index = 0

            selected_project_name = st.selectbox(
                "Оберіть проект:", 
                project_names, 
                index=default_index,
                key="sidebar_project_select"
            )
            
            # Оновлюємо поточний проект в сесії, якщо він змінився
            new_project = next((p for p in projects if p['brand_name'] == selected_project_name), None)
            if new_project and (not current_p or current_p['id'] != new_project['id']):
                st.session_state["current_project"] = new_project
                st.rerun() # Перезавантажуємо, щоб оновити основну частину

        st.markdown("### 🖥 Меню")
        
        # --- НАВІГАЦІЯ (КНОПКИ) ---
        # Використовуємо callback, щоб уникнути затримок
        
        def set_page(page_name):
            st.session_state["current_page"] = page_name
            
        # Стиль кнопок меню (щоб виглядали як на скріншоті, використовуємо звичайні кнопки, але логічно розбиті)
        
        if st.button("🚀 Дашборд", use_container_width=True):
            set_page("Дашборд")
            st.rerun()
            
        if st.button("📝 Перелік запитів", use_container_width=True):
            set_page("Перелік запитів")
            st.rerun()
            
        if st.button("🔗 Джерела", use_container_width=True):
            set_page("Джерела")
            st.rerun()
            
        if st.button("👥 Конкуренти", use_container_width=True):
            set_page("Конкуренти")
            st.rerun()

        # Активна кнопка (підсвітка - зелена)
        curr = st.session_state.get("current_page", "Дашборд")
        if curr == "Звіти":
            st.markdown(f"""<style>div[data-testid="stButton"] button {{ background-color: transparent; border: none; }} </style>""", unsafe_allow_html=True) 

        if st.button("📊 Звіти", use_container_width=True, type="primary" if curr == "Звіти" else "secondary"):
            set_page("Звіти")
            st.rerun()

        st.markdown("---")
        
        # --- ФУТЕР ---
        st.caption("Потрібна допомога?")
        st.markdown("📧 [hi@virshi.ai](mailto:hi@virshi.ai)")
        
        if st.button("🚪 Вийти з акаунту"):
            st.session_state.clear()
            st.rerun()
# =========================
# 6. DASHBOARD
# =========================

def show_competitors_page():
    """
    Сторінка глибокого конкурентного аналізу.
    ВЕРСІЯ: FINAL VISUALIZATION (Stacked Bars & Colors).
    1. Тональність: Графік Stacked Bar (🔴/⚪/🟢) для кожного бренду.
    2. Середня позиція: Топ-10 + Цільовий. Графік: Зелений (ми) vs Сірий (інші). Інверсія осі Y.
    3. Ліміт рядків: Мінімум 20.
    """
    import pandas as pd
    import plotly.express as px
    import streamlit as st
    import io
    import math

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
    
    OFFICIAL_BRAND_NAME = proj.get("brand_name", "My Brand")

    MODEL_MAPPING = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }

    # --- Ініціалізація станів пагінації ---
    if 'cp_page_list' not in st.session_state: st.session_state.cp_page_list = 1
    if 'cp_page_freq' not in st.session_state: st.session_state.cp_page_freq = 1
    if 'cp_page_sent' not in st.session_state: st.session_state.cp_page_sent = 1
    if 'cp_page_rank' not in st.session_state: st.session_state.cp_page_rank = 1

    def reset_p_list(): st.session_state.cp_page_list = 1
    def reset_p_freq(): st.session_state.cp_page_freq = 1
    def reset_p_sent(): st.session_state.cp_page_sent = 1
    def reset_p_rank(): st.session_state.cp_page_rank = 1

    st.title("👥 Аналіз Конкурентів")

    # --- 1. ЗАВАНТАЖЕННЯ ДАНИХ ---
    try:
        scans_resp = supabase.table("scan_results")\
            .select("id, provider, keyword_id, created_at")\
            .eq("project_id", proj["id"])\
            .execute()
        
        if not scans_resp.data:
            st.info("Даних немає. Запустіть сканування.")
            return
            
        df_scans = pd.DataFrame(scans_resp.data)
        
        kw_resp = supabase.table("keywords").select("id, keyword_text").eq("project_id", proj["id"]).execute()
        kw_map = {k['id']: k['keyword_text'] for k in kw_resp.data}
        df_scans['keyword_text'] = df_scans['keyword_id'].map(kw_map)

        scan_ids = df_scans['id'].tolist()
        mentions_resp = supabase.table("brand_mentions")\
            .select("*")\
            .in_("scan_result_id", scan_ids)\
            .execute()
        
        if not mentions_resp.data:
            st.info("Брендів не знайдено.")
            return

        df_mentions = pd.DataFrame(mentions_resp.data)
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

    if sel_tech_models:
        mask_model = df_full['provider'].apply(lambda x: any(t in str(x) for t in sel_tech_models))
    else:
        mask_model = df_full['provider'].apply(lambda x: False)

    if sel_kws:
        mask_kw = df_full['keyword_text'].isin(sel_kws)
    else:
        mask_kw = df_full['keyword_text'].apply(lambda x: False)

    df_filtered = df_full[mask_model & mask_kw].copy()

    if df_filtered.empty:
        st.warning("За обраними фільтрами даних немає.")
        return

    # --- 3. АГРЕГАЦІЯ ---
    mask_target = df_filtered['is_my_brand'] == True
    if mask_target.any():
        df_filtered.loc[mask_target, 'brand_name'] = OFFICIAL_BRAND_NAME

    def sentiment_to_score(s):
        if s == 'Позитивна': return 100
        if s == 'Негативна': return 0
        return 50
    
    df_filtered['sent_score_num'] = df_filtered['sentiment_score'].apply(sentiment_to_score)

    stats = df_filtered.groupby('brand_name').agg(
        Mentions=('id_x', 'count'),
        Avg_Rank=('rank_position', 'mean'),
        Avg_Sentiment_Num=('sent_score_num', 'mean'),
        Is_My_Brand=('is_my_brand', 'max')
    ).reset_index()

    # Детальна тональність
    sent_counts = df_filtered.groupby(['brand_name', 'sentiment_score']).size().unstack(fill_value=0)
    for col in ['Негативна', 'Нейтральна', 'Позитивна']:
        if col not in sent_counts.columns: sent_counts[col] = 0
            
    sent_counts['Total'] = sent_counts.sum(axis=1)
    
    # Відсотки
    sent_counts['Neg_Pct'] = (sent_counts['Негативна'] / sent_counts['Total'] * 100).fillna(0).astype(int)
    sent_counts['Neu_Pct'] = (sent_counts['Нейтральна'] / sent_counts['Total'] * 100).fillna(0).astype(int)
    sent_counts['Pos_Pct'] = (sent_counts['Позитивна'] / sent_counts['Total'] * 100).fillna(0).astype(int)

    # Строка для таблиці
    sent_counts['Тональність_Str'] = sent_counts.apply(
        lambda x: f"🔴 {x['Neg_Pct']}%   ⚪ {x['Neu_Pct']}%   🟢 {x['Pos_Pct']}%", axis=1
    )

    stats = stats.merge(sent_counts[['Тональність_Str', 'Neg_Pct', 'Neu_Pct', 'Pos_Pct']], on='brand_name', how='left')
    stats['Тональність_Str'] = stats['Тональність_Str'].fillna("🔴 0% ⚪ 0% 🟢 0%")
    stats[['Neg_Pct', 'Neu_Pct', 'Pos_Pct']] = stats[['Neg_Pct', 'Neu_Pct', 'Pos_Pct']].fillna(0)

    # --- ЛОГІКА TOP-N (Helper Function) ---
    def set_top_n_flag(df, sort_col, n=15, ascending=False):
        """
        Встановлює 'Show' = True для Top N брендів.
        Гарантовано включає цільовий бренд.
        """
        df = df.sort_values(sort_col, ascending=ascending).reset_index(drop=True)
        df['Show'] = False
        
        top_indices = df.index[:n].tolist()
        target_idx = df[df['brand_name'] == OFFICIAL_BRAND_NAME].index
        
        if not target_idx.empty:
            t_idx = target_idx[0]
            if t_idx not in top_indices:
                if len(top_indices) == n:
                    top_indices.pop()
                top_indices.append(t_idx)
        
        df.loc[top_indices, 'Show'] = True
        return df

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
        c_head, c_search, c_rows = st.columns([2, 2, 1])
        with c_head: st.markdown("##### 📋 Зведена таблиця")
        with c_search: search_list = st.text_input("🔍 Пошук бренду", key="s_list", on_change=reset_p_list)
        # Мінімум 20 рядків
        with c_rows: rows_list = st.selectbox("Рядків", [20, 50, 100, 200], key="r_list", on_change=reset_p_list)
        
        display_df = stats.copy().sort_values('Mentions', ascending=False).reset_index(drop=True)
        display_df.index = display_df.index + 1
        display_df.index.name = '#'
        display_df['Сер. Позиція'] = display_df['Avg_Rank'].apply(lambda x: f"#{x:.1f}")

        if search_list:
            display_df = display_df[display_df['brand_name'].astype(str).str.contains(search_list, case=False, na=False)]

        total_rows = len(display_df)
        total_pages = math.ceil(total_rows / rows_list)
        if st.session_state.cp_page_list > total_pages: st.session_state.cp_page_list = max(1, total_pages)
        curr_p = st.session_state.cp_page_list
        start_idx = (curr_p - 1) * rows_list
        end_idx = start_idx + rows_list
        df_page = display_df.iloc[start_idx:end_idx].copy()

        nc1, nc2, nc3 = st.columns([1, 2, 1])
        with nc1:
            if curr_p > 1: 
                if st.button("⬅️ Попередня", key="prev_list_t"): st.session_state.cp_page_list -= 1; st.rerun()
        with nc2: st.caption(f"Стор. {curr_p} з {total_pages} (Всього: {total_rows})")
        with nc3:
            if curr_p < total_pages:
                if st.button("Наступна ➡️", key="next_list_t"): st.session_state.cp_page_list += 1; st.rerun()

        def highlight_target_row(row):
            if row['brand_name'] == OFFICIAL_BRAND_NAME:
                return ['background-color: #d4edda; color: #155724; font-weight: bold'] * len(row)
            return [''] * len(row)

        cols_to_show = ['brand_name', 'Mentions', 'Сер. Позиція', 'Тональність_Str']
        styled_df = df_page[cols_to_show].style.apply(highlight_target_row, axis=1)

        dynamic_h = (len(df_page) * 35) + 38
        st.dataframe(
            styled_df,
            use_container_width=True,
            height=dynamic_h,
            column_config={
                "brand_name": "Бренд",
                "Mentions": st.column_config.ProgressColumn("Згадок", format="%d", min_value=0, max_value=int(stats['Mentions'].max())),
                "Сер. Позиція": st.column_config.TextColumn("Сер. Позиція", width="small"),
                "Тональність_Str": st.column_config.TextColumn("Тональність", width="medium")
            }
        )

        if total_rows > 20:
            bc1, bc2, bc3 = st.columns([1, 2, 1])
            with bc1:
                if curr_p > 1: 
                    if st.button("⬅️ Попередня", key="prev_list_b"): st.session_state.cp_page_list -= 1; st.rerun()
            with bc3:
                if curr_p < total_pages:
                    if st.button("Наступна ➡️", key="next_list_b"): st.session_state.cp_page_list += 1; st.rerun()

    # === TAB 2: ЧАСТОТА ЗГАДКИ ===
    with tab_freq:
        c_head, c_search, c_rows = st.columns([2, 2, 1])
        with c_head: st.markdown("##### 📊 Частота згадки (Топ-15)")
        with c_search: search_freq = st.text_input("🔍 Пошук бренду", key="s_freq", on_change=reset_p_freq)
        with c_rows: rows_freq = st.selectbox("Рядків", [20, 50, 100, 200], key="r_freq", on_change=reset_p_freq)
        
        df_for_freq = stats.copy()
        df_for_freq['Display_Name'] = df_for_freq.apply(
            lambda x: f"🟢 {x['brand_name']}" if x['brand_name'] == OFFICIAL_BRAND_NAME else x['brand_name'], axis=1
        )
        # Топ-15
        df_for_freq = set_top_n_flag(df_for_freq, 'Mentions', n=15, ascending=False)
        
        if search_freq:
            df_for_freq = df_for_freq[df_for_freq['brand_name'].astype(str).str.contains(search_freq, case=False, na=False)]

        col_table, col_chart = st.columns([1.8, 2.2])

        with col_table:
            total_rows = len(df_for_freq)
            total_pages = math.ceil(total_rows / rows_freq)
            if st.session_state.cp_page_freq > total_pages: st.session_state.cp_page_freq = max(1, total_pages)
            curr_p = st.session_state.cp_page_freq
            start_idx = (curr_p - 1) * rows_freq
            end_idx = start_idx + rows_freq
            df_page = df_for_freq.iloc[start_idx:end_idx]

            nc1, nc2, nc3 = st.columns([1, 2, 1])
            with nc1:
                if curr_p > 1: 
                    if st.button("⬅️", key="p_freq_t"): st.session_state.cp_page_freq -= 1; st.rerun()
            with nc2: st.caption(f"Стор. {curr_p}/{total_pages}")
            with nc3:
                if curr_p < total_pages: 
                    if st.button("➡️", key="n_freq_t"): st.session_state.cp_page_freq += 1; st.rerun()

            dynamic_h = (len(df_page) * 35) + 38
            edited_freq_df = st.data_editor(
                df_page[['Show', 'Display_Name', 'Mentions']],
                column_config={
                    "Show": st.column_config.CheckboxColumn("Відобразити", width="small"),
                    "Display_Name": st.column_config.TextColumn("Бренд", disabled=True),
                    "Mentions": st.column_config.ProgressColumn("Згадок", format="%d", min_value=0, max_value=int(stats['Mentions'].max())),
                },
                hide_index=True,
                use_container_width=True,
                height=dynamic_h,
                key=f"editor_freq_{curr_p}"
            )
            
            if total_rows > 20:
                bc1, bc2, bc3 = st.columns([1, 2, 1])
                with bc1:
                    if curr_p > 1: 
                        if st.button("⬅️", key="p_freq_b"): st.session_state.cp_page_freq -= 1; st.rerun()
                with bc3:
                    if curr_p < total_pages: 
                        if st.button("➡️", key="n_freq_b"): st.session_state.cp_page_freq += 1; st.rerun()

        with col_chart:
            chart_data = edited_freq_df[edited_freq_df['Show'] == True].copy()
            chart_data['Original_Name'] = chart_data['Display_Name'].apply(lambda x: x.replace("🟢 ", ""))
            
            if not chart_data.empty:
                # Додаємо колір: Зелений для нашого, Сірий/Тіл для інших
                chart_data['Color'] = chart_data['Original_Name'].apply(lambda x: '#00C896' if x == OFFICIAL_BRAND_NAME else '#90A4AE')
                
                fig = px.bar(
                    chart_data, 
                    x='Original_Name', 
                    y='Mentions',
                    text='Mentions'
                )
                fig.update_traces(marker_color=chart_data['Color'])
                fig.update_layout(xaxis_title="", yaxis_title="Кількість згадок", showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Оберіть бренди.")

    # === TAB 3: ТОНАЛЬНІСТЬ (STACKED BAR CHART) ===
    with tab_sent:
        c_head, c_search, c_rows = st.columns([2, 2, 1])
        with c_head: st.markdown("##### ⭐ Тональність (Топ-15)")
        with c_search: search_sent = st.text_input("🔍 Пошук бренду", key="s_sent", on_change=reset_p_sent)
        with c_rows: rows_sent = st.selectbox("Рядків", [20, 50, 100, 200], key="r_sent", on_change=reset_p_sent)
        
        df_for_sent = stats.copy()
        df_for_sent['Display_Name'] = df_for_sent.apply(
            lambda x: f"🟢 {x['brand_name']}" if x['brand_name'] == OFFICIAL_BRAND_NAME else x['brand_name'], axis=1
        )
        df_for_sent = set_top_n_flag(df_for_sent, 'Mentions', n=15, ascending=False)

        if search_sent:
            df_for_sent = df_for_sent[df_for_sent['brand_name'].astype(str).str.contains(search_sent, case=False, na=False)]

        col_table, col_chart = st.columns([1.8, 2.2])

        with col_table:
            total_rows = len(df_for_sent)
            total_pages = math.ceil(total_rows / rows_sent)
            if st.session_state.cp_page_sent > total_pages: st.session_state.cp_page_sent = max(1, total_pages)
            curr_p = st.session_state.cp_page_sent
            start_idx = (curr_p - 1) * rows_sent
            end_idx = start_idx + rows_sent
            df_page = df_for_sent.iloc[start_idx:end_idx]

            nc1, nc2, nc3 = st.columns([1, 2, 1])
            with nc1:
                if curr_p > 1: 
                    if st.button("⬅️", key="p_sent_t"): st.session_state.cp_page_sent -= 1; st.rerun()
            with nc2: st.caption(f"Стор. {curr_p}/{total_pages}")
            with nc3:
                if curr_p < total_pages: 
                    if st.button("➡️", key="n_sent_t"): st.session_state.cp_page_sent += 1; st.rerun()

            dynamic_h = (len(df_page) * 35) + 38
            edited_sent_df = st.data_editor(
                df_page[['Show', 'Display_Name', 'Тональність_Str']],
                column_config={
                    "Show": st.column_config.CheckboxColumn("Відобразити", width="small"),
                    "Display_Name": st.column_config.TextColumn("Бренд", disabled=True),
                    "Тональність_Str": st.column_config.TextColumn("Розподіл", disabled=True, width="medium"),
                },
                hide_index=True,
                use_container_width=True,
                height=dynamic_h,
                key=f"editor_sent_{curr_p}"
            )
            
            if total_rows > 20:
                bc1, bc2, bc3 = st.columns([1, 2, 1])
                with bc1:
                    if curr_p > 1: 
                        if st.button("⬅️", key="p_sent_b"): st.session_state.cp_page_sent -= 1; st.rerun()
                with bc3:
                    if curr_p < total_pages: 
                        if st.button("➡️", key="n_sent_b"): st.session_state.cp_page_sent += 1; st.rerun()

        with col_chart:
            # 🔥 БУДУЄМО ГРАФІК З НАКОПИЧЕННЯМ (STACKED)
            selected_rows = edited_sent_df[edited_sent_df['Show'] == True]
            selected_rows['Original_Name'] = selected_rows['Display_Name'].apply(lambda x: x.replace("🟢 ", ""))
            
            # Нам треба перетворити дані в "довгий" формат для Plotly (Brand | Sentiment | Value)
            # Беремо дані з таблиці stats, бо там є відсотки
            target_brands = selected_rows['Original_Name'].tolist()
            chart_data_src = stats[stats['brand_name'].isin(target_brands)].copy()
            
            if not chart_data_src.empty:
                # Мелтимо (розгортаємо) датафрейм
                df_melted = chart_data_src.melt(
                    id_vars=['brand_name'], 
                    value_vars=['Neg_Pct', 'Neu_Pct', 'Pos_Pct'], 
                    var_name='Sentiment_Type', 
                    value_name='Percentage'
                )
                
                # Перейменовуємо для легенди
                df_melted['Sentiment'] = df_melted['Sentiment_Type'].map({
                    'Neg_Pct': 'Негативна',
                    'Neu_Pct': 'Нейтральна',
                    'Pos_Pct': 'Позитивна'
                })
                
                # Карта кольорів
                color_map = {
                    "Негативна": "#FF5252", # Червоний
                    "Нейтральна": "#CFD8DC", # Світло-сірий
                    "Позитивна": "#00C896"   # Зелений
                }
                
                fig = px.bar(
                    df_melted,
                    x="brand_name",
                    y="Percentage",
                    color="Sentiment",
                    text="Percentage",
                    color_discrete_map=color_map,
                    # Порядок: Негатив внизу, Нейтрал, Позитив зверху (або як зручно)
                    category_orders={"Sentiment": ["Негативна", "Нейтральна", "Позитивна"]},
                    height=500
                )
                
                fig.update_traces(texttemplate='%{text}%', textposition='inside')
                fig.update_layout(
                    barmode='stack', # Робить один стовпчик з частинами
                    xaxis_title="", 
                    yaxis_title="Частка (%)", 
                    legend_title="",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Оберіть бренди.")

    # === TAB 4: СЕРЕДНЯ ПОЗИЦІЯ (TOP-10) ===
    with tab_rank:
        c_head, c_search, c_rows = st.columns([2, 2, 1])
        with c_head: st.markdown("##### 🏆 Середня позиція (Топ-10)")
        with c_search: search_rank = st.text_input("🔍 Пошук бренду", key="s_rank", on_change=reset_p_rank)
        with c_rows: rows_rank = st.selectbox("Рядків", [20, 50, 100, 200], key="r_rank", on_change=reset_p_rank)

        col_table, col_chart = st.columns([1.8, 2.2])

        with col_table:
            df_for_rank = stats.copy()
            df_for_rank['Display_Name'] = df_for_rank.apply(
                lambda x: f"🟢 {x['brand_name']}" if x['brand_name'] == OFFICIAL_BRAND_NAME else x['brand_name'], axis=1
            )
            # 🔥 Топ-10 (n=10)
            df_for_rank = set_top_n_flag(df_for_rank, 'Avg_Rank', n=10, ascending=True)

            if search_rank:
                df_for_rank = df_for_rank[df_for_rank['brand_name'].astype(str).str.contains(search_rank, case=False, na=False)]

            total_rows = len(df_for_rank)
            total_pages = math.ceil(total_rows / rows_rank)
            if st.session_state.cp_page_rank > total_pages: st.session_state.cp_page_rank = max(1, total_pages)
            curr_p = st.session_state.cp_page_rank
            start_idx = (curr_p - 1) * rows_rank
            end_idx = start_idx + rows_rank
            df_page = df_for_rank.iloc[start_idx:end_idx]

            nc1, nc2, nc3 = st.columns([1, 2, 1])
            with nc1:
                if curr_p > 1: 
                    if st.button("⬅️", key="p_rank_t"): st.session_state.cp_page_rank -= 1; st.rerun()
            with nc2: st.caption(f"Стор. {curr_p}/{total_pages}")
            with nc3:
                if curr_p < total_pages: 
                    if st.button("➡️", key="n_rank_t"): st.session_state.cp_page_rank += 1; st.rerun()

            dynamic_h = (len(df_page) * 35) + 38
            edited_rank_df = st.data_editor(
                df_page[['Show', 'Display_Name', 'Avg_Rank']],
                column_config={
                    "Show": st.column_config.CheckboxColumn("Відобразити", width="small"),
                    "Display_Name": st.column_config.TextColumn("Бренд", disabled=True),
                    "Avg_Rank": st.column_config.NumberColumn("Сер. Позиція", format="%.1f"),
                },
                hide_index=True,
                use_container_width=True,
                height=dynamic_h,
                key=f"editor_rank_{curr_p}"
            )
            
            if total_rows > 20:
                bc1, bc2, bc3 = st.columns([1, 2, 1])
                with bc1:
                    if curr_p > 1: 
                        if st.button("⬅️", key="p_rank_b"): st.session_state.cp_page_rank -= 1; st.rerun()
                with bc3:
                    if curr_p < total_pages: 
                        if st.button("➡️", key="n_rank_b"): st.session_state.cp_page_rank += 1; st.rerun()

        with col_chart:
            chart_data = edited_rank_df[edited_rank_df['Show'] == True].copy()
            chart_data['Original_Name'] = chart_data['Display_Name'].apply(lambda x: x.replace("🟢 ", ""))
            
            # Колір: Зелений (Ми) vs Сірий (Інші)
            chart_data['Color'] = chart_data['Original_Name'].apply(
                lambda x: '#00C896' if x == OFFICIAL_BRAND_NAME else '#B0BEC5'
            )

            if not chart_data.empty:
                # Бар чарт
                fig = px.bar(
                    chart_data, 
                    x='Original_Name', 
                    y='Avg_Rank',
                    text='Avg_Rank'
                )
                
                # Фарбуємо
                fig.update_traces(
                    marker_color=chart_data['Color'],
                    texttemplate='%{text:.1f}', 
                    textposition='outside'
                )
                
                fig.update_layout(
                    xaxis_title="", 
                    yaxis_title="Середня позиція (менше = краще)", 
                    showlegend=False
                )
                # 🔥 Інверсія осі Y, щоб 1 було зверху
                fig.update_yaxes(autorange="reversed")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Оберіть бренди.")

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

def generate_html_report_content(project_name, scans_data, whitelist_domains):
    """
    Генерує HTML-звіт.
    ВИПРАВЛЕНО:
    1. Таблиця брендів сортується строго за Позицією (1 -> 2 -> ...).
    2. Цільовий бренд НЕ піднімається штучно вгору, а залишається на своєму місці в ренкінгу.
    3. Рядок цільового бренду підсвічується кольором для зручності.
    """
    import pandas as pd
    from datetime import datetime
    import re
    from urllib.parse import urlparse

    current_date = datetime.now().strftime('%d.%m.%Y')

    # --- Helpers ---
    def safe_int(val):
        try: return int(float(val))
        except: return 0

    def get_domain(url):
        try:
            return urlparse(str(url)).netloc.replace('www.', '').lower()
        except:
            return ""

    def is_url_official(url, domains_list):
        if not url or not domains_list: return False
        try:
            url_lower = str(url).lower()
            for domain in domains_list:
                clean_d = str(domain).lower().replace('https://', '').replace('http://', '').replace('www.', '').strip()
                if clean_d and clean_d in url_lower:
                    return True
            return False
        except:
            return False

    def format_llm_text(text):
        if not text: return "Текст відповіді відсутній."
        txt = str(text)
        # Форматуємо жирний шрифт та списки
        txt = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', txt)
        txt = txt.replace('* ', '<br>• ')
        txt = txt.replace('\n', '<br>')
        # Підсвітка бренду у тексті - за вашим бажанням (залишив мінімальну, можна прибрати)
        return txt

    # --- UI Mapping ---
    PROVIDER_MAPPING = {
        "perplexity": "Perplexity",
        "gpt-4o": "OpenAI GPT",
        "gpt-4": "OpenAI GPT",
        "gemini-1.5-pro": "Google Gemini",
        "gemini": "Google Gemini"
    }
    
    def get_ui_provider(p):
        p_str = str(p).lower()
        for k, v in PROVIDER_MAPPING.items():
            if k in p_str: return v
        return str(p).capitalize()

    # --- Group Data ---
    data_by_provider = {}
    target_norm = str(project_name).lower().strip().split(' ')[0] if project_name else ""

    for scan in scans_data:
        prov_ui = get_ui_provider(scan.get('provider', 'Other'))
        if prov_ui not in data_by_provider:
            data_by_provider[prov_ui] = []
        
        # 1. Mentions Processing
        mentions = scan.get('brand_mentions', [])
        processed_mentions = []
        for m in mentions:
            # Визначаємо is_real_target (для підсвітки та SOV, але не для сортування!)
            b_name = str(m.get('brand_name', '')).lower().strip()
            is_db_flag = str(m.get('is_my_brand', '')).lower() in ['true', '1', 't', 'yes', 'on']
            is_text_match = (target_norm in b_name) if target_norm else False
            
            m['is_real_target'] = is_db_flag or is_text_match
            m['mention_count'] = safe_int(m.get('mention_count', 0))
            m['rank_position'] = safe_int(m.get('rank_position', 0))
            processed_mentions.append(m)
        scan['brand_mentions'] = processed_mentions

        # 2. Sources Processing
        sources = scan.get('extracted_sources', [])
        processed_sources = []
        for s in sources:
            url = s.get('url', '')
            s['is_official_calc'] = is_url_official(url, whitelist_domains)
            s['domain_clean'] = get_domain(url)
            processed_sources.append(s)
        scan['extracted_sources'] = processed_sources
        
        data_by_provider[prov_ui].append(scan)

    providers_ui = sorted(data_by_provider.keys())

    # --- CSS ---
    css_styles = '''
    @font-face { font-family: 'Golca'; src: url('') format('woff2'); font-weight: normal; font-style: normal; }
    * { box-sizing: border-box; }
    body { margin: 0; padding: 20px; background-color: #00d18f; font-family: 'Golca', 'Montserrat', sans-serif; color: #333; line-height: 1.6; }
    .content-card { background: #ffffff; border-radius: 20px; padding: 40px; max-width: 1000px; margin: 0 auto; box-shadow: 0 10px 40px rgba(0,0,0,0.15); }
    .virshi-logo-container { text-align: center; margin: 0 auto 20px auto; }
    .logo-img-real { max-width: 150px; height: auto; }
    .report-header { text-align: center; border-bottom: 2px solid #eee; padding-bottom: 20px; margin-bottom: 30px; }
    h1 { font-size: 28px; color: #2c3e50; margin: 0; font-weight: 800; }
    .subtitle { color: #888; margin-top: 10px; font-size: 14px; }
    
    .tabs-nav { display: flex; justify-content: center; gap: 10px; margin-bottom: 30px; flex-wrap: wrap; }
    .tab-btn { padding: 12px 25px; border: 2px solid #00d18f; background: #fff; color: #00d18f; border-radius: 30px; cursor: pointer; font-weight: 800; font-size: 14px; transition: all 0.3s ease; text-transform: uppercase; }
    .tab-btn.active, .tab-btn:hover { background: #00d18f; color: #fff; }
    .tab-content { display: none; animation: fadeIn 0.5s; }
    .tab-content.active { display: block; }
    @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }

    .kpi-row { display: flex; flex-wrap: wrap; justify-content: space-between; gap: 15px; margin-bottom: 20px; }
    .kpi-box { flex: 1 1 220px; border: 2px solid #00d18f; border-radius: 15px; padding: 20px; text-align: center; background: #e0f2f1; display: flex; flex-direction: column; align-items: center; justify-content: flex-start; position: relative; min-height: 200px; }
    .kpi-title { font-size: 13px; text-transform: uppercase; font-weight: bold; color: #555; margin-bottom: 10px; height: 30px; display: flex; align-items: center; justify-content: center; width: 100%; }
    .kpi-big-num { font-size: 28px; font-weight: 800; color: #2c3e50; margin-bottom: 10px; }
    .chart-container { position: relative; width: 130px; height: 130px; margin: auto; }
    .kpi-tooltip { visibility: hidden; opacity: 0; width: 220px; background-color: #2c3e50; color: #fff; text-align: center; border-radius: 8px; padding: 10px; position: absolute; z-index: 100; bottom: 105%; left: 50%; transform: translateX(-50%); font-size: 11px; transition: opacity 0.3s; pointer-events: none; }
    .kpi-box:hover .kpi-tooltip { visibility: visible; opacity: 1; }

    .summary-section { margin-top: 40px; margin-bottom: 30px; }
    .summary-header { font-size: 18px; font-weight: 800; color: #2c3e50; border-left: 5px solid #00d18f; padding-left: 15px; margin-bottom: 15px; }
    table.summary-table { width: 100%; border-collapse: collapse; font-size: 13px; border: 1px solid #eee; }
    table.summary-table th { background: #4DD0E1; color: #fff; padding: 10px; text-align: left; font-weight: 700; text-transform: uppercase; }
    table.summary-table td { padding: 10px; border-bottom: 1px solid #eee; color: #333; }
    table.summary-table tr:nth-child(even) { background-color: #f9f9f9; }

    h3 { font-size: 20px; color: #2c3e50; margin-top: 40px; margin-bottom: 20px; padding-left: 15px; border-left: 5px solid #00d18f; font-weight: 800; }

    .item-box { border: 2px solid #4DD0E1; border-radius: 15px; margin-bottom: 20px; overflow: hidden; background: #fff; }
    .accordion-trigger { background: #fff; padding: 15px 20px; display: flex; align-items: center; gap: 15px; cursor: pointer; transition: 0.3s; justify-content: space-between; }
    .accordion-trigger:hover { background-color: #f9f9f9; }
    .accordion-trigger.active { background-color: #f0fdff; border-bottom: 1px solid #eee; }
    .item-number-wrapper { width: 36px; height: 36px; background: #00d18f; border-radius: 50%; display: flex; align-items: center; justify-content: center; color: #fff; font-weight: bold; font-size: 14px; flex-shrink: 0; }
    .item-query { font-size: 15px; font-weight: bold; color: #333; flex-grow: 1; margin-left: 15px;}
    
    .cards-row { display: flex; flex-wrap: wrap; gap: 10px; padding: 20px; background: #fff; border-bottom: 1px solid #eee; }
    .metric-card { flex: 1 1 200px; background: #ffffff; border: 1px solid #e0e0e0; border-top: 4px solid #00d18f; border-radius: 8px; padding: 15px; text-align: center; }
    .mc-label { font-size: 10px; font-weight: 700; text-transform: uppercase; color: #888; margin-bottom: 5px; display:flex; align-items:center; justify-content:center; gap:5px; }
    .mc-val { font-size: 20px; font-weight: 800; color: #333; }
    .info-icon { display:inline-block; width:14px; height:14px; background:#3b82f6; color:white; border-radius:50%; font-size:10px; line-height:14px; text-align:center; cursor:help; }

    .item-response { background-color: #f9fafb; color: #1d192b; padding: 25px; font-size: 14px; text-align: left; line-height: 1.6; }
    .response-label { font-weight: bold; color: #5e35b1; margin-bottom: 15px; display: block; font-size: 16px; border-bottom: 1px dashed #5e35b1; padding-bottom: 5px; width: fit-content; }

    .detail-charts-wrapper { display: flex; flex-wrap: wrap; gap: 20px; margin-top: 30px; padding-top: 20px; border-top: 1px solid #e0e0e0; }
    .detail-chart-block { flex: 1 1 400px; min-width: 0; }
    .detail-title { font-weight: bold; font-size: 14px; margin-bottom: 10px; color: #2c3e50; border-left: 3px solid #00d18f; padding-left: 10px; }
    
    table.inner-table { width: 100%; border-collapse: collapse; font-size: 12px; border: 1px solid #eee; }
    table.inner-table th { background: #f1f3f5; padding: 8px; text-align: left; color: #555; font-weight: 600; border-bottom: 1px solid #ddd; }
    table.inner-table td { padding: 8px; border-bottom: 1px solid #eee; color: #333; }
    
    .cta-block { margin-top: 40px; padding: 20px; background-color: #e0f2f1; border: 2px solid #00d18f; border-radius: 15px; text-align: center; font-size: 12px; }
    @media (min-width: 768px) { .content-card { padding: 50px; } }
    '''

    # JS
    js_block = '''
    <script>
    Chart.defaults.font.family = "'Golca', 'Montserrat', sans-serif";
    Chart.defaults.plugins.tooltip.enabled = true;
    const colorMain = "#00d18f"; const colorOfficial = "#4DD0E1"; const colorEmpty = "#ffcdd2";

    function createDoughnut(id, val, activeColor) {
        var ctx = document.getElementById(id);
        if(!ctx) return;
        new Chart(ctx, {
            type: 'doughnut',
            data: { datasets: [{ data: [val, 100 - val], backgroundColor: [activeColor, colorEmpty], borderWidth: 0, hoverOffset: 4 }] },
            options: { layout: { padding: 10 }, responsive: true, maintainAspectRatio: false, cutout: '70%', plugins: { legend: { display: false }, tooltip: { enabled: false } } }
        });
    }
    function openTab(evt, tabName) {
        var i, tabcontent, tablinks;
        tabcontent = document.getElementsByClassName("tab-content");
        for (i = 0; i < tabcontent.length; i++) { tabcontent[i].style.display = "none"; }
        tablinks = document.getElementsByClassName("tab-btn");
        for (i = 0; i < tablinks.length; i++) { tablinks[i].className = tablinks[i].className.replace(" active", ""); }
        document.getElementById(tabName).style.display = "block";
        evt.currentTarget.className += " active";
    }
    function toggleAcc(el) {
        el.classList.toggle("active");
        var panel = el.nextElementSibling;
        if (panel.style.display === "block") { panel.style.display = "none"; } else { panel.style.display = "block"; }
    }
    window.addEventListener('load', function() { __JS_CHARTS_PLACEHOLDER__ });
    </script>
    '''

    html_template = '''<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Звіт AI Visibility</title>
<link rel="icon" type="image/png" href="https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/faviconV2.png">
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;700;800;900&display=swap" rel="stylesheet">
<style>__CSS_PLACEHOLDER__</style>
</head>
<body>
<div class="content-card">
    <div class="virshi-logo-container"><img src="https://raw.githubusercontent.com/virshi-ai/image/39ba460ec649893b9495427aa102420beb1fa48d/virshi-op_logo-main.png" class="logo-img-real" alt="VIRSHI Logo"></div>
    <div class="report-header"><h1>Звіт AI Visibility: __PROJECT_NAME__</h1><div class="subtitle">Дата формування: __DATE__</div></div>
    <div class="tabs-nav">__TABS_BUTTONS__</div>
    __TABS_CONTENT__
    <div class="cta-block"><p>Повний аудит Al Visibility включає моніторинг згадок вашого бренду в різних LLM.</p><p>Напишіть нам: <a href="mailto:hi@virshi.ai">hi@virshi.ai</a></p></div>
</div>
__JS_BLOCK__
</body>
</html>'''

    tabs_buttons_html = ""
    for i, prov in enumerate(providers_ui):
        active_cls = "active" if i == 0 else ""
        prov_id = str(prov).replace(" ", "_").replace(".", "")
        tabs_buttons_html += f'<button class="tab-btn {active_cls}" onclick="openTab(event, \'{prov_id}\')">{prov}</button>\n'

    tabs_content_html = ""
    js_charts_code = ""

    tt_sov = "Частка видимості вашого бренду у відповідях ШІ порівняно з конкурентами."
    tt_off = "Частка посилань, які ведуть на ваші офіційні ресурси."
    tt_sent = "Тональність, у якій ШІ описує бренд."
    tt_pos = "Середня позиція вашого бренду у відповідях ШІ"
    tt_brand_cov = "Відсоток запитів, у яких бренд був згаданий хоча б один раз."
    tt_domain_cov = "Відсоток запитів, у яких ШІ надав клікабельне посилання на ваш домен."

    # --- MAIN LOOP ---
    for i, prov_ui in enumerate(providers_ui):
        active_cls = "style='display:block;'" if i == 0 else "style='display:none;'"
        prov_id = str(prov_ui).replace(" ", "_").replace(".", "")
        
        provider_scans = data_by_provider[prov_ui]
        
        # --- GLOBAL CALCS ---
        all_mentions = []
        all_sources = []
        for s in provider_scans:
            all_mentions.extend(s['brand_mentions'])
            all_sources.extend(s['extracted_sources'])
            
        df_m_all = pd.DataFrame(all_mentions)
        df_s_all = pd.DataFrame(all_sources)
        
        total_queries = len(provider_scans)
        
        # 1. SOV
        sov_pct = 0
        if not df_m_all.empty:
            total_market = df_m_all['mention_count'].sum()
            my_total = df_m_all[df_m_all['is_real_target'] == True]['mention_count'].sum()
            if total_market > 0: sov_pct = (my_total / total_market * 100)
            
        # 2. Official %
        off_pct = 0
        if not df_s_all.empty:
            total_lnk = len(df_s_all)
            off_lnk = len(df_s_all[df_s_all['is_official_calc'] == True])
            if total_lnk > 0: off_pct = (off_lnk / total_lnk * 100)
            
        # 3. Brand Coverage
        brand_cov = 0
        scans_present_count = 0
        for s in provider_scans:
            found = any(m['is_real_target'] and m['mention_count'] > 0 for m in s['brand_mentions'])
            if found: scans_present_count += 1
        if total_queries > 0: brand_cov = (scans_present_count / total_queries * 100)

        # 4. Domain Coverage
        domain_cov = 0
        scans_link_count = 0
        for s in provider_scans:
             found_link = any(src['is_official_calc'] for src in s['extracted_sources'])
             if found_link: scans_link_count += 1
        if total_queries > 0: domain_cov = (scans_link_count / total_queries * 100)

        # 5. Avg Position
        avg_pos = 0
        if not df_m_all.empty:
            my_ranks = df_m_all[(df_m_all['is_real_target'] == True) & (df_m_all['rank_position'] > 0)]['rank_position']
            if not my_ranks.empty: avg_pos = my_ranks.mean()
        
        # 6. Sentiment %
        sent_html = "<span style='font-size:16px; color:#999'>Немає даних</span>"
        if not df_m_all.empty:
            valid_sent = df_m_all[(df_m_all['is_real_target'] == True) & (df_m_all['sentiment_score'] != 'Не згадано')]
            if not valid_sent.empty:
                counts = valid_sent['sentiment_score'].value_counts(normalize=True) * 100
                pos = counts.get('Позитивна', 0)
                neu = counts.get('Нейтральна', 0)
                neg = counts.get('Негативна', 0)
                sent_html = f"""
                <span style='color:#00C896'>😊 {pos:.0f}%</span> &nbsp;
                <span style='color:#FFCE56'>😐 {neu:.0f}%</span> &nbsp;
                <span style='color:#FF4B4B'>😡 {neg:.0f}%</span>
                """

        # --- SUMMARY TABLES ---
        summary_competitors_html = ""
        if not df_m_all.empty:
            comp_grp = df_m_all.groupby('brand_name').agg(
                total_mentions=('mention_count', 'sum'),
                avg_pos=('rank_position', lambda x: x[x>0].mean() if not x[x>0].empty else 0),
                sentiment=('sentiment_score', lambda x: x.mode()[0] if not x.empty else 'Не згадано')
            ).reset_index()
            # Sort by count desc
            comp_grp = comp_grp[comp_grp['total_mentions'] > 0].sort_values('total_mentions', ascending=False)
            
            rows = ""
            for _, r in comp_grp.iterrows():
                pos_val = f"{r['avg_pos']:.1f}" if r['avg_pos'] > 0 else "-"
                rows += f"<tr><td>{r['brand_name']}</td><td>{int(r['total_mentions'])}</td><td>{r['sentiment']}</td><td>{pos_val}</td></tr>"
            
            if rows:
                summary_competitors_html = f'''
                <div class="summary-section">
                    <div class="summary-header">🏆 Конкурентний аналіз</div>
                    <div class="table-responsive"><table class="summary-table"><thead><tr><th>Бренд</th><th>Згадок</th><th>Настрій</th><th>Поз.</th></tr></thead><tbody>{rows}</tbody></table></div>
                </div>'''
            
        summary_links_html = ""
        if not df_s_all.empty:
            off_links = df_s_all[df_s_all['is_official_calc'] == True]
            if not off_links.empty:
                links_grp = off_links.groupby('url').size().reset_index(name='count').sort_values('count', ascending=False)
                rows = ""
                for _, r in links_grp.iterrows():
                    rows += f"<tr><td style='word-break:break-all;'><a href='{r['url']}' target='_blank' style='color:#00d18f; text-decoration:none;'>{r['url']}</a></td><td>{r['count']}</td></tr>"
                
                summary_links_html = f'''
                <div class="summary-section">
                    <div class="summary-header">✅ Цитовані офіційні посилання</div>
                    <div class="table-responsive"><table class="summary-table"><thead><tr><th>URL</th><th>Кількість</th></tr></thead><tbody>{rows}</tbody></table></div>
                </div>'''

        summary_domains_html = ""
        if not df_s_all.empty:
            dom_grp = df_s_all.groupby('domain_clean').size().reset_index(name='count').sort_values('count', ascending=False)
            rows = ""
            for _, r in dom_grp.iterrows():
                if r['domain_clean']:
                    rows += f"<tr><td>{r['domain_clean']}</td><td>{r['count']}</td></tr>"
            
            if rows:
                summary_domains_html = f'''
                <div class="summary-section">
                    <div class="summary-header">🌐 Ренкінг доменів</div>
                    <div class="table-responsive"><table class="summary-table"><thead><tr><th>Домен</th><th>Згадок</th></tr></thead><tbody>{rows}</tbody></table></div>
                </div>'''

        # Tab Content
        tabs_content_html += f'''
        <div id="{prov_id}" class="tab-content" {active_cls}>
            <div class="kpi-row">
                <div class="kpi-box"><div class="kpi-tooltip">{tt_sov}</div><div class="kpi-title">Частка голосу (SOV)</div><div class="kpi-big-num">{sov_pct:.2f}%</div><div class="chart-container"><canvas id="chartSOV_{prov_id}"></canvas></div></div>
                <div class="kpi-box"><div class="kpi-tooltip">{tt_off}</div><div class="kpi-title">% Офіційних джерел</div><div class="kpi-big-num">{off_pct:.2f}%</div><div class="chart-container"><canvas id="chartOfficial_{prov_id}"></canvas></div></div>
                <div class="kpi-box"><div class="kpi-tooltip">{tt_sent}</div><div class="kpi-title">Загальна тональність</div><div style="margin-top:15px; font-weight:bold;">{sent_html}</div></div>
            </div>
            <div class="kpi-row">
                <div class="kpi-box"><div class="kpi-tooltip">{tt_pos}</div><div class="kpi-title">Позиція бренду</div><div class="kpi-big-num">{avg_pos:.1f}</div><div class="chart-container"><canvas id="chartPos_{prov_id}"></canvas></div></div>
                <div class="kpi-box"><div class="kpi-tooltip">{tt_brand_cov}</div><div class="kpi-title">Присутність бренду</div><div class="kpi-big-num">{brand_cov:.1f}%</div><div class="chart-container"><canvas id="chartBrandCov_{prov_id}"></canvas></div></div>
                <div class="kpi-box"><div class="kpi-tooltip">{tt_domain_cov}</div><div class="kpi-title">Згадки домену</div><div class="kpi-big-num">{domain_cov:.1f}%</div><div class="chart-container"><canvas id="chartDomainCov_{prov_id}"></canvas></div></div>
            </div>
            
            {summary_competitors_html}
            {summary_links_html}
            {summary_domains_html}
            
            <h3 style="page-break-before: always;">Детальний аналіз запитів</h3>
            <div class="accordion-wrapper">
        '''

        # --- LOOPS (Accordion) ---
        for idx, scan_row in enumerate(provider_scans):
            q_text = scan_row.get('keyword_text', 'Запит')
            
            loc_mentions = pd.DataFrame(scan_row['brand_mentions'])
            loc_sources = pd.DataFrame(scan_row['extracted_sources'])
            
            # --- Local Metrics ---
            l_tot = loc_mentions['mention_count'].sum() if not loc_mentions.empty else 0
            my_row = loc_mentions[loc_mentions['is_real_target'] == True] if not loc_mentions.empty else pd.DataFrame()
            l_my = my_row['mention_count'].sum() if not my_row.empty else 0
            
            l_sov = (l_my / l_tot * 100) if l_tot > 0 else 0
            l_count = safe_int(l_my)
            l_sent = "Не знайдено"; l_pos = "0"; l_sent_color = "#333"

            if not my_row.empty and l_my > 0:
                best = my_row.sort_values('mention_count', ascending=False).iloc[0]
                l_sent = best.get('sentiment_score', 'Не знайдено')
                vr = my_row[my_row['rank_position'] > 0]['rank_position']
                val = vr.min() if not vr.empty else None
                if pd.notnull(val): l_pos = f"#{safe_int(val)}"
            
            if l_sent == "Позитивна": l_sent_color = "#00C896"
            elif l_sent == "Негативна": l_sent_color = "#FF4B4B"

            # Detail Tables
            details_html = ""
            if not loc_mentions.empty or not loc_sources.empty:
                details_html += '<div class="detail-charts-wrapper">'
                
                # Brands Table with Correct Sorting
                if not loc_mentions.empty:
                    rows_b = ""
                    
                    # 1. Замінюємо 0 на велике число для сортування (9999), щоб 0 були в кінці
                    loc_mentions['sort_rank'] = loc_mentions['rank_position'].replace(0, 9999)
                    
                    # 2. Сортуємо: Спочатку по рангу (1, 2, 3...), потім по кількості (більше -> краще)
                    sort_b = loc_mentions.sort_values(
                        ['sort_rank', 'mention_count'], 
                        ascending=[True, False]
                    )
                    
                    has_b = False
                    for _, b in sort_b.iterrows():
                        if b['mention_count'] > 0:
                            has_b = True
                            bg = "style='background:#e6fffa; font-weight:bold;'" if b['is_real_target'] else ""
                            rows_b += f"<tr {bg}><td>{b['brand_name']}</td><td>{safe_int(b['mention_count'])}</td><td>{b['sentiment_score']}</td><td>{safe_int(b['rank_position'])}</td></tr>"
                    
                    if has_b:
                        details_html += f'<div class="detail-chart-block"><div class="detail-title">Знайдені бренди</div><div class="table-responsive"><table class="inner-table"><thead><tr><th>Бренд</th><th>Кіл.</th><th>Настрій</th><th>Поз.</th></tr></thead><tbody>{rows_b}</tbody></table></div></div>'
                    else:
                        details_html += '<div class="detail-chart-block"><div class="detail-title">Знайдені бренди</div><div style="font-size:12px; color:#999; padding:5px;">Брендів не знайдено</div></div>'

                # Sources
                if not loc_sources.empty:
                    rows_s = ""
                    for _, s in loc_sources.iterrows():
                        icon = "✅" if s['is_official_calc'] else "🔗"
                        url = str(s.get('url', ''))
                        rows_s += f"<tr><td style='word-break:break-all;'><a href='{url}' target='_blank' style='color:#00d18f; text-decoration:none;'>{url}</a></td><td>{icon}</td></tr>"
                    details_html += f'<div class="detail-chart-block"><div class="detail-title">Цитовані джерела</div><div class="table-responsive"><table class="inner-table"><thead><tr><th>URL</th><th>Тип</th></tr></thead><tbody>{rows_s}</tbody></table></div></div>'
                
                details_html += '</div>'

            raw_t = format_llm_text(scan_row.get('raw_response', ''))
            
            tabs_content_html += f'''
            <div class="item-box">
                <div class="item-header accordion-trigger" onclick="toggleAcc(this)">
                    <div class="item-number-wrapper"><span class="item-number">{idx+1}</span></div>
                    <div class="item-query">{q_text}</div>
                    <div class="accordion-arrow">▼</div>
                </div>
                <div class="accordion-content" style="display: none;">
                    <div class="cards-row">
                        <div class="metric-card"><div class="mc-label">SOV <span class="info-icon" title="Частка">%</span></div><div class="mc-val">{l_sov:.1f}%</div></div>
                        <div class="metric-card"><div class="mc-label">ЗГАДОК <span class="info-icon" title="Кількість">#</span></div><div class="mc-val">{l_count}</div></div>
                        <div class="metric-card"><div class="mc-label">ТОНАЛЬНІСТЬ <span class="info-icon" title="Настрій">☺</span></div><div class="mc-val" style="font-size:16px; color:{l_sent_color};">{l_sent}</div></div>
                        <div class="metric-card"><div class="mc-label">ПОЗИЦІЯ <span class="info-icon" title="Ранг">1</span></div><div class="mc-val">{l_pos}</div></div>
                    </div>
                    <div class="item-response">
                        <div class="response-label">Відповідь LLM:</div>
                        {raw_t}
                        {details_html}
                    </div>
                </div>
            </div>'''
        
        tabs_content_html += "</div></div>"
        
        # JS Charts
        js_charts_code += f"createDoughnut('chartSOV_{prov_id}', {sov_pct}, '#00d18f');\n"
        js_charts_code += f"createDoughnut('chartOfficial_{prov_id}', {off_pct}, '#4DD0E1');\n"
        js_charts_code += f"createDoughnut('chartBrandCov_{prov_id}', {brand_cov}, '#00d18f');\n"
        js_charts_code += f"createDoughnut('chartDomainCov_{prov_id}', {domain_cov}, '#4DD0E1');\n"
        score_pos = max(0, 11 - avg_pos) if avg_pos > 0 else 0
        js_charts_code += f"createDoughnut('chartPos_{prov_id}', {score_pos * 10}, '#00d18f');\n"

    final_js = js_block.replace("__JS_CHARTS_PLACEHOLDER__", js_charts_code)
    final_html = html_template.replace("__CSS_PLACEHOLDER__", css_styles)\
        .replace("__PROJECT_NAME__", str(project_name))\
        .replace("__DATE__", str(current_date))\
        .replace("__TABS_BUTTONS__", tabs_buttons_html)\
        .replace("__TABS_CONTENT__", tabs_content_html)\
        .replace("__JS_BLOCK__", final_js)

    return final_html
    

def show_reports_page():
    """
    Сторінка Звітів (Фінальна версія).
    Виправлено:
    - Прибрано запис в неіснуючу колонку 'created_by'.
    - Виправлено логіку підключення до БД.
    - Видалення доступне тільки в Модерації (для адмінів).
    - Оновлено дизайн кнопок та текстів.
    """
    import streamlit as st
    import pandas as pd
    from datetime import datetime
    import streamlit.components.v1 as components
    import pytz 

    kyiv_tz = pytz.timezone('Europe/Kyiv')

    st.title("📊 Звіти")

    # 1. Надійна ініціалізація Supabase
    if 'supabase' in st.session_state:
        supabase = st.session_state['supabase']
    elif 'supabase' in globals():
        supabase = globals()['supabase']
    else:
        st.error("🚨 Помилка: відсутнє підключення до БД (змінна supabase не знайдена).")
        return
    
    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Оберіть проект у сайдбарі.")
        return

    user_role = st.session_state.get("role", "user")
    is_admin = (user_role in ["admin", "super_admin"])
    
    # Вкладки
    tab_names = ["📥 Замовити звіт", "📂 Готові звіти"]
    if is_admin:
        tab_names.append("🛡️ Модерація звітів")
        
    tabs = st.tabs(tab_names)

    # =========================================================
    # ТАБ 1: ЗАМОВЛЕННЯ
    # =========================================================
    with tabs[0]:
        st.markdown("### 🚀 Генерація професійного AI-звіту")
        
        st.info("""
        **Що входить у цей звіт і яка його цінність?**
        
        Цей звіт — це комплексний аудит видимості вашого бренду в генеративних моделях (ChatGPT, Gemini, Perplexity). 
        Ми аналізуємо реальні відповіді ШІ на запити вашої цільової аудиторії.

        **Як формуються метрики:**
        1.  **Share of Voice (SOV):** Частка згадок вашого бренду порівняно з конкурентами.
        2.  **Тональність:** Відсотковий розподіл (Позитив/Нейтраль/Негатив).
        3.  **% Офіційних джерел:** Частка посилань на ваші верифіковані домени (Whitelist).
        4.  **Згадки домену:** Як часто ШІ дає прямі посилання на ваш сайт.
        
        *Звіт формується автоматично на основі останніх актуальних сканувань.*
        """)
        
        rep_name = st.text_input("Назва звіту", value=f"Звіт {proj.get('brand_name')} - {datetime.now().strftime('%d.%m.%Y')}")
        
        if st.button("✨ Сформувати звіт", type="primary"):
            with st.spinner("Аналіз даних, розрахунок метрик та генерація HTML..."):
                try:
                    # 1. Whitelist
                    wl_resp = supabase.table("official_assets").select("domain_or_url").eq("project_id", proj["id"]).execute()
                    whitelist_domains = [w['domain_or_url'] for w in wl_resp.data] if wl_resp.data else []

                    # 2. Keywords
                    kw_resp = supabase.table("keywords").select("id, keyword_text").eq("project_id", proj["id"]).execute()
                    kw_map = {k['id']: k['keyword_text'] for k in kw_resp.data} if kw_resp.data else {}
                    
                    if not kw_map:
                        st.error("У проекті немає ключових слів.")
                        st.stop()

                    # 3. Scans + Data
                    scans_resp = supabase.table("scan_results")\
                        .select("*, brand_mentions(*), extracted_sources(*)")\
                        .eq("project_id", proj["id"])\
                        .order("created_at", desc=True)\
                        .limit(2000)\
                        .execute()
                    
                    raw_scans = scans_resp.data if scans_resp.data else []
                    if not raw_scans:
                        st.error("Історія сканувань пуста.")
                        st.stop()

                    # 4. Snapshot Logic
                    processed_scans = []
                    for s in raw_scans:
                        s['keyword_text'] = kw_map.get(s['keyword_id'], "Unknown Query")
                        processed_scans.append(s)
                    
                    df_raw = pd.DataFrame(processed_scans)
                    if not df_raw.empty:
                        df_raw = df_raw.sort_values('created_at', ascending=False)
                        df_latest = df_raw.drop_duplicates(subset=['keyword_id', 'provider'], keep='first')
                        final_scans_data = df_latest.to_dict('records')
                    else:
                        final_scans_data = []

                    # 5. Generate HTML
                    html_code = generate_html_report_content(
                        proj.get('brand_name'), 
                        final_scans_data, 
                        whitelist_domains
                    )

                    # 6. Save (БЕЗ created_by, бо його немає в схемі)
                    supabase.table("reports").insert({
                        "project_id": proj["id"],
                        "report_name": rep_name,
                        "html_content": html_code,
                        "status": "pending"
                    }).execute()
                    
                    st.balloons()
                    st.success("✅ Звіт успішно сформовано! Очікуйте на модерацію.")
                    
                except Exception as e:
                    st.error(f"Помилка генерації: {e}")

    # =========================================================
    # ТАБ 2: ГОТОВІ ЗВІТИ (Перегляд)
    # =========================================================
    with tabs[1]:
        try:
            pub_resp = supabase.table("reports").select("*").eq("project_id", proj["id"]).eq("status", "published").order("created_at", desc=True).execute()
            reports = pub_resp.data if pub_resp.data else []
            
            if not reports:
                st.info("Поки що немає готових звітів.")
            else:
                for r in reports:
                    with st.expander(f"📄 {r['report_name']}", expanded=False):
                        # Кнопка завантаження (справа)
                        c_info, c_btn = st.columns([4, 1])
                        with c_btn:
                            st.download_button(
                                label="📥 Завантажити",
                                data=r['html_content'],
                                file_name=f"{r['report_name']}.html",
                                mime="text/html",
                                key=f"dl_btn_{r['id']}",
                                use_container_width=True
                            )
                        
                        # Відображення звіту
                        st.markdown("---")
                        components.html(r['html_content'], height=800, scrolling=True)
                        
        except Exception as e:
            st.error(f"Помилка завантаження: {e}")

    # =========================================================
    # ТАБ 3: МОДЕРАЦІЯ (Тільки Адмін)
    # =========================================================
    if is_admin:
        with tabs[2]:
            st.markdown("### 🛡️ Панель модератора")
            try:
                admin_resp = supabase.table("reports").select("*").eq("project_id", proj["id"]).order("created_at", desc=True).execute()
                all_reports = admin_resp.data if admin_resp.data else []
                
                if not all_reports:
                    st.info("Звітів немає.")
                else:
                    for pr in all_reports:
                        status_color = "orange" if pr['status'] == 'pending' else "green"
                        status_text = "ОЧІКУЄ" if pr['status'] == 'pending' else "ОПУБЛІКОВАНО"
                        
                        with st.container(border=True):
                            c_head, c_meta = st.columns([2, 1])
                            with c_head:
                                st.markdown(f"#### {pr['report_name']}")
                                st.markdown(f"Статус: :{status_color}[{status_text}]")
                            
                            with c_meta:
                                # Час
                                try:
                                    dt_utc = datetime.fromisoformat(pr['created_at'].replace('Z', '+00:00'))
                                    dt_kyiv = dt_utc.astimezone(kyiv_tz)
                                    fmt_time = dt_kyiv.strftime('%d.%m.%Y %H:%M')
                                except:
                                    fmt_time = pr['created_at']
                                
                                st.caption(f"📅 {fmt_time}")
                                # Автор - прибрано, бо немає колонки created_by

                            # Редактор
                            with st.expander("✏️ Редагувати код"):
                                new_html = st.text_area(
                                    "HTML Code", 
                                    value=pr['html_content'], 
                                    height=300, 
                                    key=f"edit_{pr['id']}"
                                )
                                if st.button("💾 Зберегти зміни", key=f"save_{pr['id']}"):
                                    supabase.table("reports").update({"html_content": new_html}).eq("id", pr['id']).execute()
                                    st.success("Збережено!")
                                    st.rerun()

                            # Прев'ю
                            if st.checkbox("👁️ Прев'ю", key=f"preview_{pr['id']}"):
                                components.html(pr['html_content'], height=500, scrolling=True)

                            st.divider()
                            
                            # Дії
                            ac1, ac2, ac3 = st.columns([1, 1, 3])
                            with ac1:
                                if pr['status'] != 'published':
                                    if st.button("✅ Опублікувати", key=f"pub_{pr['id']}", type="primary"):
                                        supabase.table("reports").update({"status": "published"}).eq("id", pr['id']).execute()
                                        st.success("Готово!")
                                        st.rerun()
                                else:
                                    st.button("Вже опубліковано", disabled=True, key=f"dis_{pr['id']}")
                            
                            with ac3:
                                # Кнопка видалення з унікальним ключем
                                if st.button("🗑️ Видалити", key=f"del_adm_{pr['id']}", type="secondary"):
                                    supabase.table("reports").delete().eq("id", pr['id']).execute()
                                    st.warning("Видалено.")
                                    st.rerun() # Оновлюємо сторінку одразу
            except Exception as e:
                st.error(f"Помилка адмінки: {e}")
                
def show_dashboard():
    """
    Сторінка Дашборд.
    ВЕРСІЯ: FINAL UI ADJUSTMENTS.
    1. Огляд моделей: Додано заголовок "Тональність".
    2. Конкуренти: Прибрано LLM-стовпчики, SOV/Присутність цифрами.
    3. Деталізація: Червоний великий конкурент, метрики цільового бренду.
    """
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    import streamlit as st
    from datetime import datetime, timedelta

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
            align-items: center; /* Центрування по горизонталі */
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
            font-size: 14px; /* Збільшений шрифт */
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
            
            scan_resp = supabase.table("scan_results")\
                .select("id, provider, created_at, keyword_id")\
                .eq("project_id", proj["id"])\
                .order("created_at", desc=True)\
                .execute()
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
    # 3. ОБРОБКА ДАНИХ
    # ==============================================================================
    def norm_provider(p):
        p = str(p).lower()
        if 'perplexity' in p: return 'Perplexity'
        if 'gpt' in p: return 'OpenAI GPT'
        if 'gemini' in p: return 'Google Gemini'
        return 'Other'

    scans_df['provider_ui'] = scans_df['provider'].apply(norm_provider)
    scans_df['created_at'] = pd.to_datetime(scans_df['created_at'])

    target_brand_raw = proj.get('brand_name', '').strip()
    target_brand_lower = target_brand_raw.lower()
    
    if not mentions_df.empty:
        mentions_df['mention_count'] = pd.to_numeric(mentions_df['mention_count'], errors='coerce').fillna(0)
        mentions_df['rank_position'] = pd.to_numeric(mentions_df['rank_position'], errors='coerce').fillna(0)
        
        df_full = pd.merge(mentions_df, scans_df, left_on='scan_result_id', right_on='id', suffixes=('_m', '_s'))
        
        df_full['is_target'] = df_full.apply(
            lambda x: x.get('is_my_brand', False) or (target_brand_lower in str(x.get('brand_name', '')).lower()), axis=1
        )
    else:
        df_full = pd.DataFrame()

    # ==============================================================================
    # 4. МЕТРИКИ ПО МОДЕЛЯХ (ВИПРАВЛЕНО ЗАГОЛОВОК)
    # ==============================================================================
    st.markdown("### 🌐 Огляд по моделях")
    
    def get_llm_stats(model_name):
        model_scans = scans_df[scans_df['provider_ui'] == model_name]
        if model_scans.empty: return 0, 0, (0,0,0)
        
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
            pos_c = len(my_mentions[my_mentions['sentiment_score'] == 'Позитивна'])
            neu_c = len(my_mentions[my_mentions['sentiment_score'] == 'Нейтральна'])
            neg_c = len(my_mentions[my_mentions['sentiment_score'] == 'Негативна'])
            
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
                
                # Sentiment Box (Centered with Header)
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
    # 5. ГРАФІК ДИНАМІКИ
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
    # 6. КОНКУРЕНТНИЙ АНАЛІЗ (СПРОЩЕНИЙ ВИГЛЯД)
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
                # ВАЖЛИВО: NumberColumn замість ProgressColumn
                "SOV": st.column_config.NumberColumn("Частка голосу (SOV)", format="%.1f%%", help="% від усіх згадок."),
                "Присутність": st.column_config.NumberColumn("Присутність", format="%.0f%%", help="% запитів, де знайдено бренд."),
                "Тональність": st.column_config.TextColumn("Тональність", help="Домінуюча тональність."),
            }
        )
    else:
        st.info("Немає даних для аналізу конкурентів.")

    # ==============================================================================
    # 7. ДЕТАЛЬНА СТАТИСТИКА
    # ==============================================================================
    st.write("")
    st.markdown("### 📋 Детальна статистика по запитах")
    st.caption("Метрики розраховані для вашого цільового бренду.")
    
    cols = st.columns([0.4, 2.5, 1, 1, 1, 1.2, 2])
    cols[1].markdown("**Запит**")
    cols[2].markdown("**Згадок**", help="Кількість згадок вашого бренду в цьому запиті.")
    cols[3].markdown("**SOV**", help="Ваша частка голосу в цьому запиті.")
    cols[4].markdown("**Позиція**", help="Середня позиція вашого бренду.")
    cols[5].markdown("**Тональність**", help="Переважаюча тональність вашого бренду.")
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
                has_data = True
                sorted_scans = kw_data.sort_values('created_at', ascending=False)
                latest_date = sorted_scans['created_at'].max()
                current_slice = sorted_scans[sorted_scans['created_at'] >= (latest_date - timedelta(hours=12))]

                # Наш бренд
                my_rows = current_slice[current_slice['is_target'] == True]
                my_mentions_count = my_rows['mention_count'].sum()
                tot = current_slice['mention_count'].sum()
                cur_sov = (my_mentions_count / tot * 100) if tot > 0 else 0
                
                ranks = my_rows[my_rows['rank_position'] > 0]['rank_position']
                cur_rank = ranks.mean() if not ranks.empty else 0
                
                cur_sent = my_rows['sentiment_score'].mode()[0] if not my_rows['sentiment_score'].mode().empty else "—"

                # Конкурент
                competitors = current_slice[current_slice['is_target'] == False]
                if not competitors.empty:
                    top_comp_name = competitors.groupby('brand_name')['mention_count'].sum().idxmax()
                    top_comp_val = competitors.groupby('brand_name')['mention_count'].sum().max()
                else:
                    top_comp_name = "Немає"; top_comp_val = 0
                    
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
    ВЕРСІЯ: AUTO-REFRESH (ST.FRAGMENT).
    1. Дані сканувань, графіки та таби оновлюються автоматично кожні 5 сек.
    2. Виправлено відсутній імпорт uuid.
    """
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    import streamlit as st
    from datetime import datetime, timedelta
    import numpy as np
    import time
    import re
    import uuid # 🔥 ДОДАНО: Потрібен для генерації унікальних ключів графіків
    
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
    
    # Функція нормалізації назв з бази
    def get_ui_model_name(db_name):
        for ui, db in MODEL_CONFIG.items():
            if db == db_name: return ui
        lower = str(db_name).lower()
        if "perplexity" in lower: return "Perplexity"
        if "gpt" in lower or "openai" in lower: return "OpenAI GPT"
        if "gemini" in lower or "google" in lower: return "Google Gemini"
        return db_name 

    def tooltip(text):
        return f'<span title="{text}" style="cursor:help; font-size:14px; color:#333; margin-left:4px;">ℹ️</span>'

    def normalize_url(u):
        u = str(u).strip()
        u = re.split(r'[)\]]', u)[0] 
        if not u.startswith(('http://', 'https://')): return f"https://{u}"
        return u

    # 1. ОТРИМАННЯ ДАНИХ ЗАПИТУ (СТАТИЧНА ЧАСТИНА)
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

    # ⚙️ БЛОК НАЛАШТУВАНЬ (ЗАЛИШАЄМО СТАТИЧНИМ, ЩОБ НЕ ЗБИВАТИ ВВІД)
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

    # =================================================================================
    # 🔥 LIVE SECTION: АВТО-ОНОВЛЕННЯ ДАНИХ (KPI, Charts, Tabs)
    # =================================================================================
    @st.fragment(run_every=5)
    def render_live_analytics():
        # 2. ОТРИМАННЯ ДАНИХ (Всередині фрагмента)
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
                df_scans['created_at'] = df_scans['created_at'].dt.tz_convert('Europe/Kiev')
                df_scans['date_str'] = df_scans['created_at'].dt.strftime('%Y-%m-%d %H:%M')
                
                df_scans['provider_ui'] = df_scans['provider'].apply(get_ui_model_name)
            else:
                df_scans = pd.DataFrame(columns=['scan_id', 'created_at', 'provider', 'raw_response', 'date_str', 'provider_ui'])

            # B. Mentions
            if not df_scans.empty:
                scan_ids = df_scans['scan_id'].tolist()
                if scan_ids:
                    mentions_resp = supabase.table("brand_mentions")\
                        .select("*")\
                        .in_("scan_result_id", scan_ids)\
                        .execute()
                    mentions_data = mentions_resp.data if mentions_resp.data else []
                    df_mentions = pd.DataFrame(mentions_data)
                else:
                    df_mentions = pd.DataFrame()
            else:
                df_mentions = pd.DataFrame()

            # SMART MERGE
            if not df_mentions.empty and target_brand_name:
                df_mentions['brand_clean'] = df_mentions['brand_name'].astype(str).str.lower().str.strip()
                target_norm = target_brand_name.lower().split(' ')[0]
                mask_match = df_mentions['brand_clean'].str.contains(target_norm, na=False)
                df_mentions['is_real_target'] = mask_match | (df_mentions['is_my_brand'] == True)
            elif not df_mentions.empty:
                df_mentions['is_real_target'] = df_mentions['is_my_brand']

            # C. Merge
            if not df_mentions.empty:
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

        # 3. KPI (GLOBAL)
        if not df_mentions.empty:
            my_brand_data = df_mentions[df_mentions['is_real_target'] == True]
            
            total_my_mentions = my_brand_data['mention_count'].sum()
            unique_competitors = df_mentions[df_mentions['is_real_target'] == False]['brand_name'].nunique()
            
            scan_totals = df_mentions.groupby('scan_result_id')['mention_count'].sum().reset_index()
            scan_totals.rename(columns={'mention_count': 'scan_total'}, inplace=True)
            
            my_mentions_per_scan = my_brand_data.groupby('scan_result_id')['mention_count'].sum().reset_index()
            my_mentions_per_scan.rename(columns={'mention_count': 'my_count'}, inplace=True)
            
            sov_df = pd.merge(scan_totals, my_mentions_per_scan, on='scan_result_id', how='left')
            sov_df['my_count'] = sov_df['my_count'].fillna(0)
            
            mask_nonzero = sov_df['scan_total'] > 0
            sov_df.loc[mask_nonzero, 'sov'] = (sov_df.loc[mask_nonzero, 'my_count'] / sov_df.loc[mask_nonzero, 'scan_total']) * 100
            avg_sov = sov_df['sov'].mean() if not sov_df.empty else 0
            
            valid_ranks = my_brand_data[my_brand_data['rank_position'] > 0]['rank_position']
            avg_pos = valid_ranks.mean()
            display_pos = f"#{avg_pos:.1f}" if pd.notna(avg_pos) else "-"
            
            if not my_brand_data.empty:
                active_mentions = my_brand_data[my_brand_data['mention_count'] > 0]
                if not active_mentions.empty:
                    s_counts = active_mentions['sentiment_score'].value_counts()
                    total_s = s_counts.sum()
                    pos_pct = (s_counts.get("Позитивна", 0) / total_s) * 100
                    neg_pct = (s_counts.get("Негативна", 0) / total_s) * 100
                    neu_pct = (s_counts.get("Нейтральна", 0) / total_s) * 100
                else:
                    pos_pct, neg_pct, neu_pct = 0, 0, 0
            else:
                pos_pct, neg_pct, neu_pct = 0, 0, 0
        else:
            avg_sov, total_my_mentions, unique_competitors = 0, 0, 0
            display_pos = "-"
            pos_pct, neg_pct, neu_pct = 0, 0, 0

        st.markdown("""
        <style>
            .stat-box {
                background-color: #ffffff;
                border: 1px solid #E0E0E0;
                border-top: 4px solid #8041F6; 
                border-radius: 8px;
                padding: 15px;
                text-align: center;
                box-shadow: 0 4px 10px rgba(0,0,0,0.03);
                height: 140px;
                display: flex; flex-direction: column; justify-content: center;
            }
            .stat-label { font-size: 11px; color: #888; text-transform: uppercase; font-weight: 600; margin-bottom: 5px; }
            .stat-value { font-size: 26px; font-weight: 700; color: #333; line-height: 1.2;}
            .stat-sub { font-size: 13px; color: #666; margin-top: 4px; }
        </style>
        """, unsafe_allow_html=True)

        if total_my_mentions > 0:
            sent_display = f"""
            <span style='color:#00C896'>😊 {pos_pct:.0f}%</span> &nbsp;
            <span style='color:#FFCE56'>😐 {neu_pct:.0f}%</span> &nbsp;
            <span style='color:#FF4B4B'>😡 {neg_pct:.0f}%</span>
            """
        else:
            sent_display = "<span style='color:#999'>Не згадано</span>"

        k1, k2, k3, k4 = st.columns(4)
        with k1:
            tt = tooltip("Частка голосу (SOV) — % згадок вашого бренду.")
            st.markdown(f"""<div class="stat-box"><div class="stat-label">Частка голосу (SOV) {tt}</div><div class="stat-value">{avg_sov:.1f}%</div></div>""", unsafe_allow_html=True)
        with k2:
            tt = tooltip("Всього згадок вашого бренду (та кількість унікальних брендів конкурентів).")
            st.markdown(f"""<div class="stat-box"><div class="stat-label">Згадок (Всього) {tt}</div><div class="stat-value">{int(total_my_mentions)}</div><div class="stat-sub">Конкурентів: {unique_competitors}</div></div>""", unsafe_allow_html=True)
        with k3:
            tt = tooltip("Розподіл тональності (Позитив / Нейтраль / Негатив).")
            st.markdown(f"""<div class="stat-box"><div class="stat-label">Тональність {tt}</div><div style="font-size: 14px; font-weight:600; margin-top:10px;">{sent_display}</div></div>""", unsafe_allow_html=True)
        with k4:
            tt = tooltip("Середня позиція у списку (якщо бренд знайдено).")
            st.markdown(f"""<div class="stat-box"><div class="stat-label">Сер. Позиція {tt}</div><div class="stat-value">{display_pos}</div></div>""", unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # 4. ГРАФІК ДИНАМІКИ
        st.markdown("##### 📈 Динаміка показників")

        if not df_full.empty and 'scan_id' in df_full.columns:
            totals = df_full.groupby('scan_id')['mention_count'].sum().reset_index()
            totals.rename(columns={'mention_count': 'scan_total'}, inplace=True)
            df_plot_base = pd.merge(df_full, totals, on='scan_id', how='left')
            df_plot_base['sov'] = (df_plot_base['mention_count'] / df_plot_base['scan_total'] * 100).fillna(0)
        else:
            df_plot_base = pd.DataFrame()

        with st.container(border=True):
            f_col1, f_col2, f_col3 = st.columns([1.2, 1.2, 2.5])
            with f_col1:
                metric_choice = st.selectbox("Метрика:", ["Частка голосу (SOV)", "Згадки бренду", "Позиція у списку"])
            with f_col2:
                if not df_plot_base.empty:
                    min_d = df_plot_base['created_at'].min().date()
                    max_d = df_plot_base['created_at'].max().date()
                    date_range = st.date_input("Діапазон дат:", value=(min_d, max_d), min_value=min_d, max_value=max_d)
                else:
                    date_range = None
                    st.date_input("Діапазон дат:", disabled=True)
            with f_col3:
                col_llm, col_brand = st.columns(2)
                with col_llm:
                    selected_llm_ui = st.multiselect("Фільтр по LLM:", options=ALL_MODELS_UI, default=ALL_MODELS_UI)
                with col_brand:
                    if not df_plot_base.empty:
                        all_found_brands = sorted([str(b) for b in df_plot_base['brand_name'].unique() if pd.notna(b)])
                        proj = st.session_state.get("current_project", {})
                        my_brand_name = proj.get("brand_name", "")
                        default_sel = [my_brand_name] if my_brand_name in all_found_brands else ([all_found_brands[0]] if all_found_brands else [])
                        selected_brands = st.multiselect("Фільтр по Брендах:", options=all_found_brands, default=default_sel)
                    else:
                        st.multiselect("Фільтр по Брендах:", options=[], disabled=True)

        if not df_plot_base.empty and date_range:
            if isinstance(date_range, tuple):
                if len(date_range) == 2:
                    start_d, end_d = date_range
                    mask_date = (df_plot_base['created_at'].dt.date >= start_d) & (df_plot_base['created_at'].dt.date <= end_d)
                    df_plot_base = df_plot_base[mask_date]
                elif len(date_range) == 1:
                    start_d = date_range[0]
                    mask_date = (df_plot_base['created_at'].dt.date == start_d)
                    df_plot_base = df_plot_base[mask_date]

            df_plot_base = df_plot_base[df_plot_base['provider_ui'].isin(selected_llm_ui)]
            if 'selected_brands' in locals() and selected_brands:
                df_plot_base = df_plot_base[df_plot_base['brand_name'].isin(selected_brands)]
            
            df_plot_base = df_plot_base.sort_values('created_at')

            if not df_plot_base.empty:
                if metric_choice == "Частка голосу (SOV)":
                    y_col = "sov"
                    y_title = "SOV (%)"
                    y_range = [0, 100]
                elif metric_choice == "Згадки бренду":
                    y_col = "mention_count"
                    y_title = "Кількість згадок"
                    y_range = None
                else:
                    y_col = "rank_position"
                    y_title = "Позиція"
                    y_range = None

                df_plot_base['legend_label'] = df_plot_base['brand_name'] + " (" + df_plot_base['provider_ui'] + ")"

                fig = px.line(
                    df_plot_base, 
                    x="created_at", 
                    y=y_col, 
                    color="legend_label",
                    markers=True,
                    labels={"created_at": "Час", "legend_label": "Легенда", y_col: y_title}
                )
                
                if y_range: fig.update_yaxes(range=y_range)
                if metric_choice == "Позиція у списку": fig.update_yaxes(autorange="reversed")

                fig.update_xaxes(showgrid=True, showticklabels=True, tickformat="%d.%m\n%H:%M", title_text="Час")
                fig.update_layout(height=350, hovermode="x unified", margin=dict(l=0, r=0, t=10, b=0), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            else:
                st.info("Немає даних за обраними критеріями.")
        else:
            st.info("Історія сканувань порожня або не обрано дати.")

        st.markdown("---")

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
                        st.write("") 
                        st.write("")
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
                loc_sov = 0
                loc_mentions = 0
                loc_sent = "Не згадано"
                loc_rank_str = "-"
                
                current_scan_mentions = pd.DataFrame()
                if not df_mentions.empty:
                    current_scan_mentions = df_mentions[df_mentions['scan_result_id'] == selected_scan_id]
                
                if not current_scan_mentions.empty:
                    total_in_scan = current_scan_mentions['mention_count'].sum()
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
                if loc_sent == "Позитивна": sent_color = "#00C896"
                elif loc_sent == "Негативна": sent_color = "#FF4B4B"
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
                proj = st.session_state.get("current_project", {})
                brand_name = proj.get("brand_name", "")
                
                if raw_text:
                    final_html = raw_text
                    if brand_name:
                        highlight_span = f"<span style='background-color:#dcfce7; color:#166534; font-weight:bold; padding:0 4px; border-radius:4px;'>{brand_name}</span>"
                        final_html = final_html.replace(brand_name, highlight_span)
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
                            st.plotly_chart(
                                fig_brands,
                                use_container_width=True,
                                config={'displayModeBar': False},
                                key=f"brands_chart_{selected_scan_id}_{str(uuid.uuid4())[:8]}" # унікальний ключ
                            )
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
                                df_src['domain'] = df_src['url'].apply(lambda x: str(x).split('/')[2] if x and '//' in str(x) else 'unknown')
                            
                            df_src['url'] = df_src['url'].apply(normalize_url)
                            if 'is_official' in df_src.columns:
                                df_src['status_text'] = df_src['is_official'].apply(lambda x: "✅ Офіційне" if x is True else "🔗 Зовнішнє")
                            else:
                                df_src['status_text'] = "🔗 Зовнішнє"

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
                                st.plotly_chart(
                                    fig_src, 
                                    use_container_width=True, 
                                    config={'displayModeBar': False},
                                    key=f"src_chart_{selected_scan_id}_{str(uuid.uuid4())[:8]}"
                                )

                            with c_src_table:
                                st.dataframe(
                                    df_grouped_src[['url', 'count']],
                                    use_container_width=True, hide_index=True,
                                    column_config={
                                        "url": st.column_config.LinkColumn("Посилання", width="large", validate="^https?://"),
                                        "count": st.column_config.NumberColumn("К-сть", width="small")
                                    }
                                )
                        else:
                            st.info("URL не знайдено.")
                    else:
                        st.info("ℹ️ Джерел не знайдено.")
                except Exception as e:
                    st.error(f"Помилка завантаження джерел: {e}")

    # 🔥 ВИКЛИК LIVE ФРАГМЕНТА
    render_live_analytics()



def show_keywords_page():
    """
    Сторінка списку запитів.
    ВЕРСІЯ: ADDED 'PASTE LIST' TAB.
    Додано можливість масового додавання запитів списком з опцією запуску аналізу.
    """
    import pandas as pd
    import streamlit as st
    from datetime import datetime
    import time
    import io 
    import re 
    
    # Ініціалізація лічильника для примусового оновлення UI
    if "bulk_update_counter" not in st.session_state:
        st.session_state["bulk_update_counter"] = 0

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
        
        # 🔥 ДОДАНО НОВУ ВКЛАДКУ "📋 Вставити списком"
        tab_manual, tab_paste, tab_import, tab_export, tab_auto = st.tabs(["✍️ Ввести вручну", "📋 Вставити списком", "📥 Імпорт (Excel / URL)", "📤 Експорт (Excel)", "⚙️ Автозапуск"])

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

        # --- TAB 2: ВСТАВИТИ СПИСКОМ (НОВИЙ ФУНКЦІОНАЛ) ---
        with tab_paste:
            st.info("💡 Вставте список запитів. Кожен новий запит — з нового рядка.")
            paste_text = st.text_area("Список запитів", height=150, key="kw_paste_area", placeholder="купити квитки\nвідгуки про бренд\nнайкращі ціни")
            
            st.write("---")
            c_paste_models, c_paste_btn1, c_paste_btn2 = st.columns([2, 1.5, 1.5])
            
            with c_paste_models:
                selected_models_paste = st.multiselect("LLM для запуску:", list(MODEL_MAPPING.keys()), default=["Perplexity"], key="paste_multiselect")
            
            with c_paste_btn1:
                st.write("")
                st.write("")
                if st.button("📥 Тільки зберегти", use_container_width=True, key="btn_paste_save"):
                    if paste_text:
                        lines = [line.strip() for line in paste_text.split('\n') if line.strip()]
                        if lines:
                            try:
                                insert_data = [{
                                    "project_id": proj["id"], "keyword_text": kw, "is_active": True, 
                                    "is_auto_scan": False, "frequency": "daily"
                                } for kw in lines]
                                
                                supabase.table("keywords").insert(insert_data).execute()
                                st.success(f"Успішно збережено {len(lines)} запитів!")
                                time.sleep(1.5)
                                st.rerun()
                            except Exception as e:
                                st.error(f"Помилка збереження: {e}")
                        else:
                            st.warning("Список пустий.")
                    else:
                        st.warning("Поле пусте.")

            with c_paste_btn2:
                st.write("")
                st.write("")
                if st.button("🚀 Зберегти та Аналізувати", type="primary", use_container_width=True, key="btn_paste_run"):
                    if paste_text:
                        lines = [line.strip() for line in paste_text.split('\n') if line.strip()]
                        if lines:
                            try:
                                insert_data = [{
                                    "project_id": proj["id"], "keyword_text": kw, "is_active": True, 
                                    "is_auto_scan": False, "frequency": "daily"
                                } for kw in lines]
                                
                                res = supabase.table("keywords").insert(insert_data).execute()
                                if res.data:
                                    with st.spinner(f"Обробка {len(lines)} запитів..."):
                                        if 'n8n_trigger_analysis' in globals():
                                            my_bar = st.progress(0, text="Запуск...")
                                            total = len(lines)
                                            for i, kw in enumerate(lines):
                                                n8n_trigger_analysis(proj["id"], [kw], proj.get("brand_name"), models=selected_models_paste)
                                                my_bar.progress((i + 1) / total)
                                                time.sleep(0.3)
                                        st.success("Успішно збережено та запущено!")
                                        time.sleep(2)
                                        st.rerun()
                            except Exception as e:
                                st.error(f"Помилка процесу: {e}")
                        else:
                            st.warning("Список пустий.")
                    else:
                        st.warning("Поле пусте.")

        # --- TAB 3: ІМПОРТ EXCEL / URL ---
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
                        st.error("🚨 Відсутня бібліотека `openpyxl`.")
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

        # --- TAB 4: ЕКСПОРТ EXCEL ---
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

        # --- TAB 5: АВТОЗАПУСК (МАСОВЕ НАЛАШТУВАННЯ) ---
        with tab_auto:
            st.markdown("##### ⚙️ Масове налаштування автозапуску")
            
            allow_cron_global = proj.get('allow_cron', False)
            if not allow_cron_global:
                st.error("🔒 Автозапуск недоступний для цього проекту. Зверніться до адміністратора.")
            else:
                st.info("Тут ви можете керувати автоскануванням для **всіх** запитів одночасно.")

                c_freq, c_btn = st.columns([2, 1.5])
                
                with c_freq:
                    freq_map = {"Щодня": "daily", "Щотижня": "weekly", "Щомісяця": "monthly"}
                    selected_freq_ui = st.selectbox("Оберіть частоту для всіх запитів:", list(freq_map.keys()))
                    selected_freq_db = freq_map[selected_freq_ui]

                with c_btn:
                    st.write("") 
                    st.write("")
                    
                    if st.button("✅ Застосувати частоту та Увімкнути", type="primary", use_container_width=True):
                        try:
                            supabase.table("keywords").update({
                                "is_auto_scan": True,
                                "frequency": selected_freq_db
                            }).eq("project_id", proj["id"]).execute()
                            
                            st.session_state["bulk_update_counter"] += 1
                            
                            st.success(f"Оновлено! Всі запити будуть скануватися: {selected_freq_ui}")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Помилка оновлення: {e}")

                if st.button("⛔ Вимкнути автосканування для всіх", use_container_width=True):
                      try:
                        supabase.table("keywords").update({
                            "is_auto_scan": False
                        }).eq("project_id", proj["id"]).execute()

                        st.session_state["bulk_update_counter"] += 1
                        
                        st.warning("Автосканування вимкнено для всіх запитів.")
                        time.sleep(1)
                        st.rerun()
                      except Exception as e:
                        st.error(f"Помилка: {e}")
                
                st.markdown("---")
                st.markdown("""
                **ℹ️ Як це працює:**
                1. **✅ Застосувати:** Активує автозапуск (`ON`) і встановлює обрану частоту для **всіх** запитів.
                2. **⛔ Вимкнути всі:** Деактивує автозапуск (`OFF`) для всіх запитів.
                """)

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

    update_suffix = st.session_state.get("bulk_update_counter", 0)

    # Функція-фрагмент (оновлюється незалежно)
    @st.fragment(run_every=5)
    def render_live_dashboard(keywords_data, proj_data, suffix_val):
        
        # --- LIVE DATA FETCH ---
        try:
            fresh_scans = supabase.table("scan_results").select("keyword_id, created_at").eq("project_id", proj_data["id"]).order("created_at", desc=True).execute()
            fresh_map = {}
            if fresh_scans.data:
                for s in fresh_scans.data:
                    if s['keyword_id'] not in fresh_map:
                        fresh_map[s['keyword_id']] = s['created_at']
            
            for k in keywords_data:
                k['last_scan_date'] = fresh_map.get(k['id'], "1970-01-01T00:00:00+00:00")
        except Exception:
            pass

        # --- SORTING ---
        c_sort, _ = st.columns([2, 4])
        with c_sort:
            sort_option = st.selectbox("Сортувати за:", 
                                     ["Найновіші (Додані)", "Найстаріші (Додані)", "Нещодавно проскановані", "Давно не скановані"], 
                                     label_visibility="collapsed")

        sorted_kws = keywords_data.copy()
        if sort_option == "Найновіші (Додані)": sorted_kws.sort(key=lambda x: x['created_at'], reverse=True)
        elif sort_option == "Найстаріші (Додані)": sorted_kws.sort(key=lambda x: x['created_at'], reverse=False)
        elif sort_option == "Нещодавно проскановані": sorted_kws.sort(key=lambda x: x['last_scan_date'], reverse=True)
        elif sort_option == "Давно не скановані": sorted_kws.sort(key=lambda x: x['last_scan_date'], reverse=False)

        current_page_ids = [str(k['id']) for k in sorted_kws]

        # --- STATE CALLBACKS ---
        def master_checkbox_change():
            new_state = st.session_state.select_all_master_key
            for kid in current_page_ids:
                st.session_state[f"chk_{kid}"] = new_state

        def child_checkbox_change():
            all_selected = True
            for kid in current_page_ids:
                if not st.session_state.get(f"chk_{kid}", False):
                    all_selected = False
                    break
            st.session_state.select_all_master_key = all_selected

        for kid in current_page_ids:
            key = f"chk_{kid}"
            if key not in st.session_state:
                st.session_state[key] = False

        if "select_all_master_key" not in st.session_state:
            st.session_state.select_all_master_key = False

        # --- ПАНЕЛЬ ДІЙ ---
        with st.container(border=True):
            c_check, c_models, c_btn = st.columns([0.5, 3, 1.5])
            
            with c_check:
                st.write("") 
                st.checkbox("Всі", key="select_all_master_key", on_change=master_checkbox_change)
            
            with c_models:
                all_models = list(MODEL_MAPPING.keys())
                bulk_models = st.multiselect(
                    "ЛЛМ для запуску:", 
                    all_models, 
                    default=all_models, 
                    label_visibility="collapsed", 
                    key="bulk_models_selector_v6"
                )
            
            with c_btn:
                if st.button("🚀 Аналізувати обрані", use_container_width=True, type="primary"):
                    selected_texts = []
                    for k in sorted_kws:
                        if st.session_state.get(f"chk_{k['id']}", False):
                            selected_texts.append(k['keyword_text'])
                    
                    if selected_texts:
                        try:
                            if 'n8n_trigger_analysis' in globals():
                                my_bar = st.progress(0, text="Ініціалізація...")
                                total = len(selected_texts)
                                for i, txt in enumerate(selected_texts):
                                    my_bar.progress((i / total), text=f"Відправка: {txt}...")
                                    n8n_trigger_analysis(proj_data["id"], [txt], proj_data.get("brand_name"), models=bulk_models)
                                    time.sleep(0.2)
                                my_bar.progress(1.0, text="Готово!")
                                st.success(f"Запущено {total} завдань.")
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("Функція запуску не знайдена.")
                        except Exception as e:
                            st.error(f"Помилка: {e}")
                    else:
                        st.warning("Оберіть хоча б один запит.")

        # --- ТАБЛИЦЯ ---
        h_chk, h_num, h_txt, h_cron, h_date, h_act = st.columns([0.4, 0.5, 3.2, 2, 1.2, 1.3])
        h_txt.markdown("**Запит**")
        h_cron.markdown("**Автозапуск**")
        h_date.markdown("**Останній аналіз**")
        h_act.markdown("**Видалити**")

        allow_cron_global = proj_data.get('allow_cron', False)

        for idx, k in enumerate(sorted_kws, start=1):
            k_id_str = str(k['id'])
            
            with st.container(border=True):
                c1, c2, c3, c4, c5, c6 = st.columns([0.4, 0.5, 3.2, 2, 1.2, 1.3])
                
                with c1:
                    st.write("") 
                    st.checkbox("", key=f"chk_{k_id_str}", on_change=child_checkbox_change)
                
                with c2:
                    st.markdown(f"<div class='green-number'>{idx}</div>", unsafe_allow_html=True)
                
                with c3:
                    if st.button(k['keyword_text'], key=f"lnk_{k_id_str}", help="Деталі"):
                        st.session_state["focus_keyword_id"] = k["id"]
                        st.rerun()
                
                with c4:
                    cron_c1, cron_c2 = st.columns([0.8, 1.2])
                    is_auto_db = k.get('is_auto_scan', False)
                    
                    with cron_c1:
                        if allow_cron_global:
                            toggle_key = f"auto_{k_id_str}_{suffix_val}"
                            new_auto = st.toggle("Авто", value=is_auto_db, key=toggle_key, label_visibility="collapsed")
                            if new_auto != is_auto_db:
                                update_kw_field(k['id'], "is_auto_scan", new_auto)
                        else:
                            st.toggle("Авто", value=False, key=f"auto_dis_{k_id_str}", disabled=True, label_visibility="collapsed")
                            st.caption("🔒")

                    with cron_c2:
                        if allow_cron_global and (is_auto_db or new_auto): 
                            current_freq = k.get('frequency', 'daily')
                            freq_options = ["daily", "weekly", "monthly"]
                            try: idx_f = freq_options.index(current_freq)
                            except: idx_f = 0
                            
                            freq_key = f"freq_{k_id_str}_{suffix_val}"
                            new_freq = st.selectbox("Freq", freq_options, index=idx_f, key=freq_key, label_visibility="collapsed")
                            if new_freq != current_freq:
                                update_kw_field(k['id'], "frequency", new_freq)

                with c5:
                    st.write("")
                    date_iso = k.get('last_scan_date')
                    formatted_date = format_kyiv_time(date_iso)
                    st.caption(f"{formatted_date}")

                with c6:
                    st.write("")
                    del_confirm_key = f"del_confirm_{k_id_str}"
                    if del_confirm_key not in st.session_state: st.session_state[del_confirm_key] = False

                    if not st.session_state[del_confirm_key]:
                        if st.button("🗑️", key=f"pre_del_{k_id_str}"):
                            st.session_state[del_confirm_key] = True
                            st.rerun()
                    else:
                        dc1, dc2 = st.columns(2)
                        if dc1.button("✅", key=f"yes_del_{k_id_str}", type="primary"):
                            try:
                                supabase.table("scan_results").delete().eq("keyword_id", k["id"]).execute()
                                supabase.table("keywords").delete().eq("id", k["id"]).execute()
                                st.success("OK")
                                st.session_state[del_confirm_key] = False
                                time.sleep(0.5)
                                st.rerun()
                            except:
                                st.error("Error")
                        if dc2.button("❌", key=f"no_del_{k_id_str}"):
                            st.session_state[del_confirm_key] = False
                            st.rerun()

    # Запускаємо фрагмент
    render_live_dashboard(keywords, proj, update_suffix)


# =========================
# 9. SIDEBAR
# =========================

def show_sources_page():
    """
    Сторінка джерел.
    ВЕРСІЯ: FIXED ENUM & DESIGN UPDATE.
    1. Виправлено помилку 'invalid input value for enum'.
    2. Дизайн редагування змінено на стиль 'список карток' (як у запитах).
    3. Додано мапінг типів (Ukr -> Eng).
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

    # --- CSS для зелених номерів (дублюємо тут про всяк випадок) ---
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
    </style>
    """, unsafe_allow_html=True)

    st.title("🔗 Джерела")

    # --- MAPPING ТИПІВ (UI -> DB) ---
    TYPE_UI_TO_DB = {
        "Веб-сайт": "website",
        "Соціальні мережі": "social",
        "Стаття": "article",
        "Інше": "other"
    }
    # Зворотній мапінг (DB -> UI)
    TYPE_DB_TO_UI = {v: k for k, v in TYPE_UI_TO_DB.items()}

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

    # Формуємо список для логіки (для підрахунку)
    assets_list_dicts = []
    for item in raw_assets:
        # Конвертуємо тип з БД в UI (english -> ukrainian)
        db_type = item.get("type", "website")
        ui_type = TYPE_DB_TO_UI.get(db_type, "Веб-сайт")
        
        assets_list_dicts.append({
            "Домен": item.get("domain_or_url", ""), 
            "Мітка": ui_type
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

        # --- РЕДАКТОР WHITELIST (НОВИЙ ДИЗАЙН) ---
        st.subheader("⚙️ Керування списком (Whitelist)")
        
        if "edit_whitelist_mode" not in st.session_state:
            st.session_state["edit_whitelist_mode"] = False
        
        # Ініціалізація змінної для редагування
        if "temp_assets" not in st.session_state:
            st.session_state["temp_assets"] = []

        # --- ВІДОБРАЖЕННЯ ТАБЛИЦІ (View Mode) ---
        if not st.session_state["edit_whitelist_mode"]:
            # Готуємо DataFrame для перегляду
            if assets_list_dicts:
                df_assets = pd.DataFrame(assets_list_dicts)
            else:
                df_assets = pd.DataFrame(columns=["Домен", "Мітка"])

            # Рахуємо статистику
            if not df_master.empty:
                def get_stat_whitelist(dom):
                    matches = df_master[df_master['url'].astype(str).str.contains(dom.lower(), case=False, na=False)]
                    return len(matches)
                df_assets['Згадок'] = df_assets['Домен'].apply(get_stat_whitelist)
            else:
                df_assets['Згадок'] = 0

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
                # Завантажуємо поточні дані в temp_assets для редагування
                st.session_state["temp_assets"] = assets_list_dicts.copy()
                st.rerun()
        
        # --- РЕЖИМ РЕДАГУВАННЯ (Custom Design) ---
        else:
            st.info("Додайте або видаліть домени. Натисніть 'Зберегти' для застосування змін.")
            
            # Якщо список пустий, додаємо один порожній рядок
            if not st.session_state["temp_assets"]:
                st.session_state["temp_assets"].append({"Домен": "", "Мітка": "Веб-сайт"})

            # Відображаємо список карток
            for i, asset in enumerate(st.session_state["temp_assets"]):
                with st.container(border=True):
                    c_num, c_dom, c_type, c_del = st.columns([0.5, 5, 3, 1])
                    
                    with c_num:
                        st.markdown(f"<div class='green-number'>{i+1}</div>", unsafe_allow_html=True)
                    
                    with c_dom:
                        new_domain = st.text_input(
                            "Домен", 
                            value=asset["Домен"], 
                            key=f"asset_dom_{i}", 
                            label_visibility="collapsed",
                            placeholder="example.com"
                        )
                        st.session_state["temp_assets"][i]["Домен"] = new_domain
                    
                    with c_type:
                        new_type = st.selectbox(
                            "Тип", 
                            options=list(TYPE_UI_TO_DB.keys()), 
                            index=list(TYPE_UI_TO_DB.keys()).index(asset["Мітка"]) if asset["Мітка"] in TYPE_UI_TO_DB else 0,
                            key=f"asset_type_{i}", 
                            label_visibility="collapsed"
                        )
                        st.session_state["temp_assets"][i]["Мітка"] = new_type

                    with c_del:
                        if st.button("🗑️", key=f"del_asset_{i}"):
                            st.session_state["temp_assets"].pop(i)
                            st.rerun()

            # Кнопка додавання
            if st.button("➕ Додати джерело"):
                st.session_state["temp_assets"].append({"Домен": "", "Мітка": "Веб-сайт"})
                st.rerun()

            st.divider()

            # Кнопки дії
            c1, c2 = st.columns([1, 4])
            with c1:
                if st.button("💾 Зберегти", type="primary"):
                    try:
                        # 1. Видаляємо старі записи
                        supabase.table("official_assets").delete().eq("project_id", proj["id"]).execute()
                        
                        # 2. Формуємо нові дані (конвертуємо UI -> DB)
                        insert_data = []
                        for item in st.session_state["temp_assets"]:
                            d_val = str(item["Домен"]).strip()
                            if d_val:
                                # Конвертація "Веб-сайт" -> "website"
                                db_type_val = TYPE_UI_TO_DB.get(item["Мітка"], "website")
                                
                                insert_data.append({
                                    "project_id": proj["id"],
                                    "domain_or_url": d_val,
                                    "type": db_type_val
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

def show_my_projects_page():
    """
    Сторінка 'Мої проекти'.
    ВЕРСІЯ: EDIT PROJECT NAME IN LIST.
    Додано можливість редагувати назву проекту (олівець -> інпут -> зберегти).
    """
    import streamlit as st
    import pandas as pd
    from datetime import datetime
    import requests
    import re
    import time
    import uuid
    
    # --- КОНСТАНТИ ---
    N8N_GEN_URL = "https://virshi.app.n8n.cloud/webhook/webhook/generate-prompts"

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
        .stTabs [data-baseweb="tab-list"] { gap: 10px; }
        
        /* Стиль для кнопки редагування, щоб вона була компактною */
        button[kind="secondary"] {
            padding: 0px 10px !important;
            border: none !important;
        }
    </style>
    """, unsafe_allow_html=True)

    # --- ПІДКЛЮЧЕННЯ ---
    if 'supabase' in st.session_state:
        supabase = st.session_state['supabase']
    elif 'supabase' in globals():
        supabase = globals()['supabase']
    else:
        st.error("🚨 Помилка підключення до БД.")
        return

    user = st.session_state.get("user")
    if not user:
        st.error("Потрібна авторизація.")
        return
        
    # Ім'я автора
    user_details = st.session_state.get("user_details", {})
    author_name = f"{user_details.get('first_name', '')} {user_details.get('last_name', '')}".strip()
    if not author_name: author_name = user.email

    # --- ХЕЛПЕР: ГЕНЕРАЦІЯ ---
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
                except ValueError: return []
            else:
                st.error(f"Error: {response.status_code}")
                return []
        except Exception as e:
            st.error(f"Connection error: {e}")
            return []

    # --- STATE ---
    if "new_proj_keywords" not in st.session_state:
        st.session_state["new_proj_keywords"] = [] 
    if "my_proj_reset_id" not in st.session_state:
        st.session_state["my_proj_reset_id"] = 0
    if "edit_proj_id" not in st.session_state:
        st.session_state["edit_proj_id"] = None

    for item in st.session_state["new_proj_keywords"]:
        if "id" not in item: item["id"] = str(uuid.uuid4())

    st.title("📂 Мої проекти")
    
    tab1, tab2 = st.tabs(["📋 Активні проекти", "➕ Створити проект"])

    # ========================================================
    # ТАБ 1: СПИСОК ПРОЕКТІВ
    # ========================================================
    with tab1:
        try:
            projs_resp = supabase.table("projects").select("*").eq("user_id", user.id).order("created_at", desc=True).execute()
            projects = projs_resp.data if projs_resp.data else []

            if not projects:
                st.info("У вас поки немає створених проектів.")
            else:
                for p in projects:
                    with st.container(border=True):
                        col_left, col_center, col_right = st.columns([1.3, 2, 2])

                        # --- 1. Лого + Назва (Editable) ---
                        with col_left:
                            # Логіка отримання чистого домену
                            clean_d = None
                            if p.get('domain'):
                                # Очищаємо домен від зайвого
                                clean_d = p['domain'].lower().replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0]

                            # Формування основного URL логотипу
                            logo_url_src = None
                            if p.get('logo_url'):
                                logo_url_src = p['logo_url']
                            elif clean_d:
                                logo_url_src = f"https://cdn.brandfetch.io/{clean_d}"
                            
                            # Резервний логотип (Google Favicon)
                            backup_logo = f"https://www.google.com/s2/favicons?domain={clean_d}&sz=128" if clean_d else ""

                            # Відображення через HTML (ВИПРАВЛЕНО СИНТАКСИС)
                            if logo_url_src:
                                # Пишемо в один рядок, використовуючи одинарні лапки для Python і подвійні для HTML
                                # Для JS всередині HTML використовуємо екрановані лапки \'
                                img_html = f'<img src="{logo_url_src}" style="width: 80px; height: 80px; object-fit: contain; border-radius: 8px; border: 1px solid #eee; padding: 5px;" onerror="this.onerror=null; this.src=\'{backup_logo}\';">'
                                st.markdown(img_html, unsafe_allow_html=True)
                            else:
                                st.markdown("🖼️ *No Logo*")
                            
                            st.write("")
                            
                            # 🔥 ЛОГІКА РЕДАГУВАННЯ НАЗВИ
                            current_name = p.get('project_name') or p.get('brand_name') or 'Без назви'
                            
                            if st.session_state["edit_proj_id"] == p['id']:
                                # Режим редагування
                                new_p_name = st.text_input("Назва", value=current_name, key=f"edit_inp_{p['id']}", label_visibility="collapsed")
                                
                                c_save, c_canc = st.columns([1, 1])
                                if c_save.button("💾", key=f"save_{p['id']}", help="Зберегти"):
                                    if new_p_name and new_p_name != current_name:
                                        try:
                                            supabase.table("projects").update({"project_name": new_p_name}).eq("id", p['id']).execute()
                                            st.toast("Назву успішно змінено!", icon="✅")
                                            st.session_state["edit_proj_id"] = None
                                            time.sleep(0.5)
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"Помилка: {e}")
                                    else:
                                        st.session_state["edit_proj_id"] = None
                                        st.rerun()
                                        
                                if c_canc.button("❌", key=f"cncl_{p['id']}", help="Скасувати"):
                                    st.session_state["edit_proj_id"] = None
                                    st.rerun()
                            else:
                                # Режим перегляду (Текст + Олівець)
                                c_txt, c_btn = st.columns([0.8, 0.2])
                                with c_txt:
                                    st.markdown(f"**{current_name}**")
                                with c_btn:
                                    if st.button("✏️", key=f"edit_{p['id']}", help="Редагувати назву"):
                                        st.session_state["edit_proj_id"] = p['id']
                                        st.rerun()
                            
                            created_dt = p.get('created_at', '')[:10]
                            st.caption(f"📅 {created_dt}")
                            st.caption(f"👤 {author_name}")

                        # --- 2. Деталі ---
                        with col_center:
                            st.markdown(f"**Бренд:** {p.get('brand_name', '-')}")
                            st.markdown(f"**Домен:** `{p.get('domain', '-')}`")
                            st.markdown(f"**Галузь:** {p.get('industry', '-')}")
                            
                            prods = p.get('products') or p.get('description') or '-'
                            if len(prods) > 100: prods_display = prods[:100] + "..."
                            else: prods_display = prods
                            st.markdown(f"**Послуги:** {prods_display}")
                            
                            status_p = p.get('status', 'trial').upper()
                            color_s = "orange" if status_p == "TRIAL" else "green"
                            st.markdown(f"Статус: **:{color_s}[{status_p}]**")

                        # --- 3. Дії ---
                        with col_right:
                            try:
                                assets_resp = supabase.table("official_assets").select("domain_or_url").eq("project_id", p['id']).execute()
                                sources = [a['domain_or_url'] for a in assets_resp.data] if assets_resp.data else []
                            except: sources = []
                            
                            with st.expander(f"🔗 Джерела ({len(sources)})"):
                                for s in sources: st.markdown(f"- `{s}`")

                            try:
                                kw_resp = supabase.table("keywords").select("id", count="exact").eq("project_id", p['id']).execute()
                                kw_count = kw_resp.count if kw_resp.count is not None else len(kw_resp.data)
                            except: kw_count = 0
                            
                            st.markdown(f"**Кількість запитів:** `{kw_count}`")

                            st.write("")
                            if st.button(f"➡️ Відкрити проект", key=f"open_proj_{p['id']}", type="primary", use_container_width=True):
                                st.toast(f"🔄 Перемикання на проект: **{current_name}**...", icon="✅")
                                
                                keys_to_clear = ["focus_keyword_id", "new_proj_keywords", "analysis_results"]
                                for key in keys_to_clear:
                                    if key in st.session_state: del st.session_state[key]

                                st.session_state["current_project"] = p
                                if "menu_id_counter" not in st.session_state: st.session_state["menu_id_counter"] = 0
                                st.session_state["menu_id_counter"] += 1

                                time.sleep(0.7)
                                st.rerun()

        except Exception as e:
            st.error(f"Помилка завантаження проектів: {e}")

    # ========================================================
    # ТАБ 2: СТВОРЕННЯ ПРОЕКТУ
    # ========================================================
    with tab2:
        st.markdown("##### 🚀 Створення нового проекту")
        
        rk = st.session_state["my_proj_reset_id"]
        
        c1, c2 = st.columns(2)
        new_brand_val = c1.text_input("Назва бренду (для AI) *", key=f"mp_brand_{rk}", placeholder="Наприклад: Nova Poshta")
        new_domain_val = c2.text_input("Домен *", key=f"mp_domain_{rk}", placeholder="novaposhta.ua")
        
        c3, c4 = st.columns(2)
        def_proj_name = f"{new_brand_val} Audit" if new_brand_val else ""
        new_proj_name_val = c3.text_input("Назва проекту (Внутрішня) *", value=def_proj_name, key=f"mp_pname_{rk}")
        new_industry_val = c4.text_input("Галузь *", key=f"mp_ind_{rk}", placeholder="напр. Логістика")

        c5, c6 = st.columns([1, 2])
        new_region_val = c5.selectbox("Регіон", ["Ukraine", "USA", "Europe", "Global"], key=f"mp_region_{rk}")
        new_products_val = c6.text_area("Продукти/Послуги (Опис) *", placeholder="Основні послуги для AI...", height=68, key=f"mp_prod_{rk}")
        
        st.divider()
        st.markdown("###### 📝 Наповнення семантичного ядра (Keywords)")
        
        kw_tabs = st.tabs(["✨ AI Генерація", "📥 Імпорт (Excel/URL)", "📋 Вставити списком", "✍️ Додати вручну"])
        
        # --- TAB A: AI ---
        with kw_tabs[0]:
            st.caption("Автоматичне створення запитів на основі опису продуктів.")
            if st.button("✨ Згенерувати запити", key=f"mp_btn_gen_{rk}"):
                if new_domain_val and new_industry_val and new_products_val and new_brand_val: 
                    with st.spinner("AI аналізує бренд..."):
                        generated_kws = trigger_keyword_generation(new_brand_val, new_domain_val, new_industry_val, new_products_val)
                    if generated_kws:
                        for kw in generated_kws:
                            st.session_state["new_proj_keywords"].append({"id": str(uuid.uuid4()), "keyword": kw})
                        st.success(f"Додано {len(generated_kws)} запитів!")
                    else: st.warning("AI не повернув запитів.")
                else: st.warning("⚠️ Заповніть всі поля вище.")

        # --- TAB B: ІМПОРТ ---
        with kw_tabs[1]:
            st.caption("Завантажте файл або посилання.")
            import_source = st.radio("Джерело:", ["Файл (.xlsx)", "Посилання (URL)"], horizontal=True, key=f"mp_imp_src_{rk}")
            df_upload = None
            if import_source == "Файл (.xlsx)":
                uploaded_file = st.file_uploader("Оберіть файл", type=["xlsx", "csv"], key=f"mp_file_{rk}")
                if uploaded_file:
                    try: 
                        if uploaded_file.name.endswith('.csv'): df_upload = pd.read_csv(uploaded_file)
                        else: df_upload = pd.read_excel(uploaded_file)
                    except Exception as e: st.error(f"Помилка файлу: {e}")
            else:
                import_url = st.text_input("Посилання (CSV/Google Sheet):", key=f"mp_url_{rk}")
                if import_url:
                    try:
                        if "docs.google.com" in import_url:
                            match = re.search(r'/d/([a-zA-Z0-9-_]+)', import_url)
                            if match:
                                sheet_id = match.group(1)
                                csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
                                df_upload = pd.read_csv(csv_url)
                        elif import_url.endswith(".csv"): df_upload = pd.read_csv(import_url)
                        elif import_url.endswith(".xlsx"): df_upload = pd.read_excel(import_url)
                    except: st.error("Помилка URL")

            if df_upload is not None:
                target_col = df_upload.columns[0]
                cols_lower = [str(c).lower().strip() for c in df_upload.columns]
                if "keyword" in cols_lower: target_col = df_upload.columns[cols_lower.index("keyword")]
                imp_kws = df_upload[target_col].dropna().astype(str).tolist()
                if st.button(f"📥 Імпортувати {len(imp_kws)} запитів", key=f"mp_add_imp_{rk}"):
                    for kw in imp_kws:
                        st.session_state["new_proj_keywords"].append({"id": str(uuid.uuid4()), "keyword": kw})
                    st.success("Імпортовано!")
                    st.rerun()

        # --- TAB C: СПИСОК ---
        with kw_tabs[2]:
            paste_text = st.text_area("Вставте список (кожен з нового рядка)", height=150, key=f"mp_paste_{rk}")
            if st.button("📋 Додати список", key=f"mp_btn_paste_{rk}"):
                if paste_text:
                    lines = [line.strip() for line in paste_text.split('\n') if line.strip()]
                    for line in lines:
                        st.session_state["new_proj_keywords"].append({"id": str(uuid.uuid4()), "keyword": line})
                    st.success(f"Додано {len(lines)} запитів!")
                    st.rerun()

        # --- TAB D: ВРУЧНУ ---
        with kw_tabs[3]:
            c_man1, c_man2 = st.columns([4, 1])
            manual_kw = c_man1.text_input("Запит", key=f"mp_man_kw_{rk}", placeholder="Введіть запит...")
            c_man2.write("") 
            c_man2.write("") 
            if c_man2.button("➕", key=f"mp_btn_man_{rk}"):
                if manual_kw:
                    st.session_state["new_proj_keywords"].append({"id": str(uuid.uuid4()), "keyword": manual_kw})
                    st.rerun()

        # --- СПИСОК ---
        st.write("")
        st.markdown("###### 📋 Ваш список для збереження:")
        
        keywords_list = st.session_state["new_proj_keywords"]
        if not keywords_list:
            st.info("Список порожній.")
        else:
            for i, item in enumerate(keywords_list):
                unique_key = item['id']
                with st.container(border=True):
                    c_num, c_txt, c_act = st.columns([0.5, 8, 1])
                    with c_num: st.markdown(f"<div class='green-number'>{i+1}</div>", unsafe_allow_html=True)
                    with c_txt:
                        new_val = st.text_input("kw", value=item['keyword'], key=f"kw_input_{unique_key}", label_visibility="collapsed")
                        if new_val != item['keyword']:
                            for k in st.session_state["new_proj_keywords"]:
                                if k['id'] == unique_key: k['keyword'] = new_val
                    with c_act:
                        if st.button("🗑️", key=f"del_btn_{unique_key}"):
                            st.session_state["new_proj_keywords"] = [k for k in st.session_state["new_proj_keywords"] if k['id'] != unique_key]
                            st.rerun()
            
            if st.button("🗑️ Очистити весь список", key=f"mp_clear_all_{rk}", type="secondary"):
                st.session_state["new_proj_keywords"] = []
                st.rerun()

        st.divider()
        
        # --- ДІЇ ---
        col_llm, col_act = st.columns(2)
        with col_llm:
            ui_llm_options = ["OpenAI GPT", "Google Gemini", "Perplexity"]
            selected_llms = st.multiselect("Активувати LLM", ui_llm_options, default=["OpenAI GPT", "Google Gemini"], key=f"mp_llms_{rk}")
        
        with col_act:
            st.caption("Дія:")
            b1, b2 = st.columns(2)
            save_only = b1.button("💾 Зберегти проект", use_container_width=True)
            save_run = b2.button("🚀 Зберегти та Запустити", type="primary", use_container_width=True)

# ЛОГІКА ЗБЕРЕЖЕННЯ (ЗАМІНИТИ ВЕСЬ ЦЕЙ БЛОК)
        if save_only or save_run:
            final_project_name = new_proj_name_val if new_proj_name_val else new_brand_val
            
            if new_domain_val and new_industry_val and new_brand_val:
                try:
                    uid = st.session_state.user.id
                    
                    # 1. Створюємо проект
                    new_proj_data = {
                        "user_id": uid, "brand_name": new_brand_val, "project_name": final_project_name,
                        "domain": new_domain_val, "industry": new_industry_val, "products": new_products_val,
                        "status": "trial", "allow_cron": True if save_run else False, "region": new_region_val,
                        "created_at": datetime.now().isoformat()
                    }
                    res_proj = supabase.table("projects").insert(new_proj_data).execute()
                    
                    if res_proj.data:
                        new_proj_id = res_proj.data[0]['id']
                        
                        # 2. Whitelist
                        try:
                            clean_d = new_domain_val.replace("https://", "").replace("http://", "").replace("www.", "").strip().rstrip("/")
                            supabase.table("official_assets").insert({"project_id": new_proj_id, "domain_or_url": clean_d, "type": "website"}).execute()
                        except: pass
                        
                        # 3. Keywords
                        final_kws_clean = [k['keyword'].strip() for k in keywords_list if k['keyword'].strip()]
                        if final_kws_clean:
                            kws_data = [{"project_id": new_proj_id, "keyword_text": kw, "is_active": True} for kw in final_kws_clean]
                            supabase.table("keywords").insert(kws_data).execute()

                        # 4. Встановлюємо проект в сесію (важливо для нових юзерів)
                        st.session_state["current_project"] = res_proj.data[0]

                        # 5. ЗАПУСК АНАЛІЗУ (ПОШТУЧНО)
                        if save_run:
                            if 'n8n_trigger_analysis' in globals():
                                my_bar = st.progress(0, text="Ініціалізація...")
                                
                                # Рахуємо загальну к-сть операцій
                                total_ops = len(final_kws_clean) * len(selected_llms)
                                if total_ops == 0: total_ops = 1 # Щоб не ділити на 0
                                current_op = 0
                                
                                # Цикл: Слова -> Моделі
                                for kw_item in final_kws_clean:
                                    for model_item in selected_llms:
                                        current_op += 1
                                        prog_val = min(current_op / total_ops, 1.0)
                                        my_bar.progress(prog_val, text=f"Аналіз: {kw_item} ({model_item})...")
                                        
                                        # Виклик функції (вона має приймати список, тому [kw_item])
                                        n8n_trigger_analysis(
                                            project_id=new_proj_id, 
                                            keywords=[kw_item], 
                                            brand_name=new_brand_val, 
                                            models=[model_item]
                                        )
                                        time.sleep(0.2) # Пауза між запитами
                                
                                my_bar.progress(1.0, text="Готово!")
                                st.toast(f"✅ Проект '{new_brand_val}' створено! Аналіз запущено.", icon="🚀")
                            else:
                                st.error("Функція аналізу не знайдена.")
                        else:
                            st.toast(f"✅ Проект '{new_brand_val}' успішно збережено!", icon="💾")

                        # 6. Очищення та перенаправлення
                        st.session_state["new_proj_keywords"] = []
                        st.session_state["my_proj_reset_id"] += 1
                        
                        # Примусово перекидаємо на вкладку "Мої проекти" (список)
                        st.session_state["force_redirect_to"] = "Мої проекти"
                        
                        time.sleep(1.5)
                        st.rerun()
                except Exception as e: 
                    st.error(f"Помилка створення: {e}")
            else: 
                st.warning("Заповніть обов'язкові поля.")
                

def show_history_page():
    """
    Сторінка історії сканувань.
    ВЕРСІЯ: PROFILES MAPPING.
    1. Бере user_email з scan_results.
    2. Шукає власника в таблиці 'profiles'.
    3. Формує ПІБ (first_name + last_name).
    """
    import pandas as pd
    import streamlit as st
    from datetime import datetime, timedelta
    import pytz
    import math

    # Налаштування часового поясу
    KYIV_TZ = pytz.timezone('Europe/Kiev')

    # Функція для скидання сторінки
    def reset_page():
        st.session_state.history_page_number = 1

    if 'history_page_number' not in st.session_state:
        st.session_state.history_page_number = 1

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

            # 2. Scans (Беремо user_email)
            scans_resp = supabase.table("scan_results")\
                .select("id, created_at, provider, keyword_id, user_email")\
                .eq("project_id", proj["id"])\
                .order("created_at", desc=True)\
                .limit(1000)\
                .execute()
            
            scans_data = scans_resp.data if scans_resp.data else []
            
            if not scans_data:
                st.info("Історія сканувань порожня.")
                return

            scan_ids = [s['id'] for s in scans_data]

            # 🔥 3. ОТРИМАННЯ ПІБ З ТАБЛИЦІ PROFILES
            unique_emails = list(set([s['user_email'] for s in scans_data if s.get('user_email')]))
            email_to_name_map = {}

            if unique_emails:
                try:
                    # ⚠️ Змінено таблицю на 'profiles'
                    p_resp = supabase.table("profiles")\
                        .select("email, first_name, last_name")\
                        .in_("email", unique_emails)\
                        .execute()
                    
                    if p_resp.data:
                        for p in p_resp.data:
                            f_n = p.get('first_name', '') or ''
                            l_n = p.get('last_name', '') or ''
                            full_n = f"{f_n} {l_n}".strip()
                            
                            # Якщо ім'я знайдене, записуємо його в мапу
                            if full_n and p.get('email'):
                                email_to_name_map[p['email']] = full_n
                except Exception:
                    # Якщо таблиці profiles немає або помилка доступу
                    pass

            # 4. Mentions
            m_resp = supabase.table("brand_mentions")\
                .select("scan_result_id, is_my_brand, mention_count")\
                .in_("scan_result_id", scan_ids)\
                .execute()
            mentions_df = pd.DataFrame(m_resp.data) if m_resp.data else pd.DataFrame()

            # 5. Sources
            s_resp = supabase.table("extracted_sources")\
                .select("scan_result_id, is_official")\
                .in_("scan_result_id", scan_ids)\
                .execute()
            sources_df = pd.DataFrame(s_resp.data) if s_resp.data else pd.DataFrame()

        except Exception as e:
            if "column scan_results.user_email does not exist" in str(e):
                st.error("⚠️ Відсутня колонка `user_email` у таблиці scan_results.")
            else:
                st.error(f"Помилка завантаження даних: {e}")
            return

    # --- 3. ОБРОБКА ДАНИХ ---
    df_scans = pd.DataFrame(scans_data)

    # 🔥 ЛОГІКА ІНІЦІАТОРА
    def resolve_initiator(email_val):
        # 1. Якщо емейл пустий -> Авто
        if pd.isna(email_val) or str(email_val).strip() == "" or str(email_val).lower() == "none":
            return "🤖 Автосканування"
        
        # 2. Якщо ми знайшли ім'я у profiles -> Виводимо ПІБ
        if email_val in email_to_name_map:
            return f"👤 {email_to_name_map[email_val]}"
        
        # 3. Якщо імені не знайшли (профіль не заповнений) -> Виводимо Email
        return f"👤 {email_val}"
    
    # Застосовуємо, якщо колонка є
    if 'user_email' in df_scans.columns:
        df_scans['initiator'] = df_scans['user_email'].apply(resolve_initiator)
    else:
        df_scans['initiator'] = "🤖 Автосканування"

    # Провайдери
    PROVIDER_MAP = {"gpt-4o": "OpenAI", "gpt-4-turbo": "OpenAI", "gemini-1.5-pro": "Gemini", "perplexity": "Perplexity"}
    df_scans['provider'] = df_scans['provider'].replace(PROVIDER_MAP)
    
    # Ключові слова
    df_scans['keyword'] = df_scans['keyword_id'].map(kw_map).fillna("Видалений запит")
    
    # Timezone Fix
    df_scans['created_at_dt'] = pd.to_datetime(df_scans['created_at']).dt.tz_convert(KYIV_TZ)
    
    # Merge (Безпечне злиття)
    if not mentions_df.empty:
        brands_count = mentions_df.groupby('scan_result_id').size().reset_index(name='total_brands')
        my_mentions = mentions_df[mentions_df['is_my_brand'] == True].groupby('scan_result_id')['mention_count'].sum().reset_index(name='my_mentions_count')
        
        df_scans = df_scans.merge(brands_count, left_on='id', right_on='scan_result_id', how='left')
        if 'scan_result_id' in df_scans.columns: df_scans = df_scans.drop(columns=['scan_result_id'])
        
        df_scans = df_scans.merge(my_mentions, left_on='id', right_on='scan_result_id', how='left')
        if 'scan_result_id' in df_scans.columns: df_scans = df_scans.drop(columns=['scan_result_id'])
    else:
        df_scans['total_brands'] = 0
        df_scans['my_mentions_count'] = 0

    if not sources_df.empty:
        links_count = sources_df.groupby('scan_result_id').size().reset_index(name='total_links')
        off_count = sources_df[sources_df['is_official'] == True].groupby('scan_result_id').size().reset_index(name='official_links')
        
        df_scans = df_scans.merge(links_count, left_on='id', right_on='scan_result_id', how='left')
        if 'scan_result_id' in df_scans.columns: df_scans = df_scans.drop(columns=['scan_result_id'])
        
        df_scans = df_scans.merge(off_count, left_on='id', right_on='scan_result_id', how='left')
        if 'scan_result_id' in df_scans.columns: df_scans = df_scans.drop(columns=['scan_result_id'])
    else:
        df_scans['total_links'] = 0
        df_scans['official_links'] = 0

    df_scans = df_scans.fillna(0)

    # --- 4. ФІЛЬТРИ ---
    st.markdown("### 🔍 Фільтрація")
    
    now_kyiv = datetime.now(KYIV_TZ).date()
    
    if not df_scans.empty:
        min_date_avail = df_scans['created_at_dt'].min().date()
        max_date_avail = max(df_scans['created_at_dt'].max().date(), now_kyiv) + timedelta(days=1)
    else:
        min_date_avail = now_kyiv
        max_date_avail = now_kyiv + timedelta(days=1)

    c1, c2, c3, c4 = st.columns([1, 1.2, 1, 0.8])
    
    with c1:
        all_providers = df_scans['provider'].unique().tolist()
        sel_providers = st.multiselect("Модель", all_providers, default=all_providers, on_change=reset_page)
    
    with c2:
        default_start = now_kyiv - timedelta(days=30)
        sel_dates = st.date_input(
            "Період",
            value=(default_start, now_kyiv),
            min_value=min_date_avail - timedelta(days=365),
            max_value=max_date_avail
        )
        
    with c3:
        sort_opts = ["Найновіші", "Найстаріші", "Більше згадок", "Офіц. джерела"]
        sel_sort = st.selectbox("Сортування", sort_opts, on_change=reset_page)

    with c4:
        rows_per_page = st.selectbox("Рядків на стор.", [10, 20, 50, 100, 200], index=0, on_change=reset_page)

    # --- ЗАСТОСУВАННЯ ФІЛЬТРІВ ---
    mask = df_scans['provider'].isin(sel_providers)
    
    if isinstance(sel_dates, tuple):
        if len(sel_dates) == 2:
            start_d, end_d = sel_dates
            mask &= (df_scans['created_at_dt'].dt.date >= start_d)
            mask &= (df_scans['created_at_dt'].dt.date <= end_d)
        elif len(sel_dates) == 1:
            mask &= (df_scans['created_at_dt'].dt.date == sel_dates[0])
        
    df_filtered = df_scans[mask].copy()

    # Сортування
    if sel_sort == "Найновіші": df_filtered = df_filtered.sort_values('created_at_dt', ascending=False)
    elif sel_sort == "Найстаріші": df_filtered = df_filtered.sort_values('created_at_dt', ascending=True)
    elif sel_sort == "Більше згадок": df_filtered = df_filtered.sort_values('my_mentions_count', ascending=False)
    elif sel_sort == "Офіц. джерела": df_filtered = df_filtered.sort_values('official_links', ascending=False)

    # --- 5. ПАГІНАЦІЯ ---
    total_rows = len(df_filtered)
    total_pages = math.ceil(total_rows / rows_per_page)
    
    if st.session_state.history_page_number > total_pages:
        st.session_state.history_page_number = max(1, total_pages)
    
    current_page = st.session_state.history_page_number
    start_idx = (current_page - 1) * rows_per_page
    end_idx = start_idx + rows_per_page
    
    df_display_page = df_filtered.iloc[start_idx:end_idx].copy()

    # --- 6. ВІДОБРАЖЕННЯ (AUTO HEIGHT) ---
    st.divider()
    
    p_col1, p_col2, p_col3 = st.columns([1, 2, 1])
    with p_col1:
        if current_page > 1:
            if st.button("⬅️ Попередня", key="hist_prev_top"):
                st.session_state.history_page_number -= 1
                st.rerun()
    with p_col2:
        st.markdown(f"<div style='text-align: center; padding-top: 5px;'>Сторінка <b>{current_page}</b> з <b>{total_pages}</b> (Всього: {total_rows})</div>", unsafe_allow_html=True)
    with p_col3:
        if current_page < total_pages:
            if st.button("Наступна ➡️", key="hist_next_top"):
                st.session_state.history_page_number += 1
                st.rerun()

    if 'created_at_dt' in df_display_page.columns:
        df_display_page['created_at_dt'] = df_display_page['created_at_dt'].dt.strftime('%d.%m.%Y %H:%M')

    cols_to_show = ['created_at_dt', 'keyword', 'provider', 'total_brands', 'total_links', 'my_mentions_count', 'official_links', 'initiator']
    df_show = df_display_page[[c for c in cols_to_show if c in df_display_page.columns]]

    # Авто-висота
    dynamic_height = (len(df_show) * 35) + 38

    st.dataframe(
        df_show,
        use_container_width=True,
        hide_index=True,
        height=dynamic_height,
        column_config={
            "created_at_dt": "Дата (Kyiv)",
            "keyword": st.column_config.TextColumn("Запит", width="medium"),
            "provider": "LLM",
            "total_brands": st.column_config.NumberColumn("Бренди", help="Кількість знайдених конкурентів"),
            "total_links": st.column_config.NumberColumn("Посил.", help="Всього джерел"),
            "my_mentions_count": st.column_config.NumberColumn("Згадки", help="Згадки нашого бренду"),
            "official_links": st.column_config.NumberColumn("Офіц.", help="Офіційні джерела"),
            "initiator": st.column_config.TextColumn("Ініціатор", help="Хто запустив", width="medium")
        }
    )

    if total_rows > 10:
        st.write("")
        b_col1, b_col2, b_col3 = st.columns([1, 2, 1])
        with b_col1:
            if current_page > 1:
                if st.button("⬅️ Попередня", key="hist_prev_btm"):
                    st.session_state.history_page_number -= 1
                    st.rerun()
        with b_col3:
            if current_page < total_pages:
                if st.button("Наступна ➡️", key="hist_next_btm"):
                    st.session_state.history_page_number += 1
                    st.rerun()


def sidebar_menu():
    """
    Бокове меню навігації.
    ВЕРСІЯ: ADDED 'MY PROJECTS'.
    1. Додано пункт 'Мої проекти' перед Дашбордом.
    2. Іконка 'folder' для проектів.
    """
    from streamlit_option_menu import option_menu
    import streamlit as st
    
    # Отримуємо дані з сесії
    proj = st.session_state.get("current_project")
    user = st.session_state.get("user")
    user_details = st.session_state.get("user_details", {}) 
    
    user_email = user.email if user else "guest@virshi.ai"
    
    first_name = user_details.get("first_name", "")
    last_name = user_details.get("last_name", "")
    full_name = f"{first_name} {last_name}".strip()
    if not full_name: full_name = "Користувач"

    proj_name = proj.get("brand_name", "No Project") if proj else "Оберіть проект"
    proj_id = proj.get("id", "") if proj else ""
    proj_domain = proj.get("domain", "") if proj else ""

    with st.sidebar:
        # 🔥 CSS FIX
        st.markdown("""
            <style>
                /* Обнуляємо відступи контейнера, щоб контент був зверху */
                [data-testid="stSidebarBody"] {
                    padding-top: 0rem !important;
                }
                section[data-testid="stSidebar"] .block-container {
                    padding-top: 1rem !important;
                    margin-top: 0rem !important;
                }
                
                /* 🔥 КНОПКА ЗГОРТАННЯ */
                [data-testid="stSidebarHeader"] {
                    padding-top: 0rem !important;
                    height: 0rem !important;
                    
                    /* Фіксуємо позицію навпроти профілю */
                    position: absolute;
                    top: 135px !important; 
                    right: 10px !important;
                    
                    /* Робимо видимою та клікабельною */
                    z-index: 999999 !important;
                    pointer-events: auto !important;
                    background-color: transparent;
                    width: auto !important;
                }
                /* Колір іконки стрілочки */
                [data-testid="stSidebarHeader"] button {
                    color: #666 !important;
                }
                [data-testid="stSidebarHeader"] button:hover {
                    color: #00C896 !important;
                }
            </style>
        """, unsafe_allow_html=True)

        # 1. Логотип + AI VISIBILITY (Нормальне позиціонування)
        st.markdown(f"""
            <div style="text-align: center; margin-bottom: 5px;">
                <img src="https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png" width="160" style="display: inline-block;">
                <div style="margin-top: 5px; font-size: 18px; font-weight: bold; color: #333; letter-spacing: 0.5px;">AI Visibility</div>
            </div>
            
            <div style="margin-top: 20px; border-top: 1px solid #E0E0E0;"></div>
            <div style="margin-top: 15px;"></div>
        """, unsafe_allow_html=True)

        # 2. ПРОФІЛЬ (Текст)
        st.markdown(f"""
        <div style='line-height: 1.2; margin-bottom: 10px; padding-right: 40px;'>
            <div style='font-size: 12px; color: rgba(49, 51, 63, 0.6); margin-bottom: 2px;'>Ви авторизовані як:</div>
            <div style='font-weight: 600; font-size: 16px; color: #31333F;'>{full_name}</div>
            <div style='font-size: 12px; color: rgba(49, 51, 63, 0.6);'>{user_email}</div>
        </div>
        """, unsafe_allow_html=True)
        
        st.write("") 

        # --- БЛОК ПРОЕКТУ ---
        logo_url = None
        backup_logo_url = None
        clean_d = None

        if proj and proj_domain:
            clean_d = proj_domain.lower().replace('https://', '').replace('http://', '').replace('www.', '')
            if '/' in clean_d: clean_d = clean_d.split('/')[0]
            
            logo_url = f"https://cdn.brandfetch.io/{clean_d}"
            backup_logo_url = f"https://www.google.com/s2/favicons?domain={clean_d}&sz=128"

        if proj and proj_name != "Оберіть проект":
            if logo_url:
                col_brand_img, col_brand_txt = st.columns([0.25, 0.75])
                with col_brand_img:
                    img_html = f'<img src="{logo_url}" style="width: 45px; height: 45px; object-fit: contain; border-radius: 5px; pointer-events: none;" onerror="this.onerror=null; this.src=\'{backup_logo_url}\';">'
                    st.markdown(img_html, unsafe_allow_html=True)
                
                with col_brand_txt:
                    # 🔥 ОНОВЛЕНО: Великий шрифт назви проекту
                    html_content = f"""
                    <div style='line-height: 1.1; display: flex; flex-direction: column; justify-content: center; height: 48px;'>
                        <div style='font-weight: bold; font-size: 20px; color: #31333F; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;'>{proj_name}</div>
                        <div style='font-size: 12px; color: #888;'>{clean_d if clean_d else ''}</div>
                    </div>
                    """
                    st.markdown(html_content, unsafe_allow_html=True)
            else:
                st.markdown(f"### 📁 {proj_name}")
                if clean_d: st.caption(clean_d)

        # Відступ перед меню
        st.markdown("<div style='height: 20px;'></div>", unsafe_allow_html=True)

        # 4. Меню
        options = [
            "Мої проекти",      # <--- НОВИЙ ПУНКТ
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
            "folder",           # <--- ІКОНКА ДЛЯ "МОЇ ПРОЕКТИ"
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

        if st.session_state.get("role") in ["admin", "super_admin"]:
            options.append("Адмін")
            icons.append("shield-lock")

        default_idx = 0
        redirect_target = st.session_state.get("force_redirect_to")
        
        if redirect_target and redirect_target in options:
            default_idx = options.index(redirect_target)
            del st.session_state["force_redirect_to"]
        
        menu_refresh_id = st.session_state.get("menu_id_counter", 0)

        selected = option_menu(
            "Меню",
            options,
            icons=icons,
            menu_icon="cast",
            default_index=default_idx,
            key=f"main_menu_nav_{menu_refresh_id}", 
            styles={
                "container": {"padding": "0!important", "background-color": "transparent"},
                "icon": {"color": "grey", "font-size": "16px"}, 
                "nav-link": {"font-size": "14px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
                "nav-link-selected": {"background-color": "#00C896"},
            }
        )
        
        st.divider()

        # 5. Сапорт
        st.caption("Потрібна допомога?")
        st.markdown("📧 **hi@virshi.ai**")

        # 6. Статус
        if proj:
            st.write("")
            status = proj.get("status", "trial").upper()
            color = "orange" if status == "TRIAL" else "green" if status == "ACTIVE" else "red"
            
            st.markdown(f"Статус: **:{color}[{status}]**")
            st.caption(f"ID: `{proj_id}`")
            
            if st.session_state.get("is_impersonating"):
                st.info("🕵️ Admin Mode")

        st.write("")
        if st.button("🚪 Вийти з акаунту", use_container_width=True):
            if 'logout' in globals():
                logout()
            else:
                st.session_state.clear()
                st.rerun()

    return selected

def show_auth_page():
    """
    Відображає сторінку входу/реєстрації з дизайном Virshi.
    Викликає глобальні функції login_user та register_user.
    """
    # Стилізація сторінки
    st.markdown("""
    <style>
        /* Фон сторінки */
        .stApp {
            background-color: #F4F7F6;
        }
        
        /* Центрування контейнера форми */
        [data-testid="stForm"] {
            background-color: #ffffff;
            padding: 40px;
            border-radius: 12px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.08);
            border: 1px solid #EAEAEA;
        }

        /* Стилізація полів вводу */
        .stTextInput > div > div > input {
            border-radius: 8px;
            border: 1px solid #e0e0e0;
            padding: 10px;
        }

        /* Основна кнопка (Virshi Green) */
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
        
        /* Стилізація вкладок */
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

    # Розмітка колонок для центрування
    col_l, col_center, col_r = st.columns([1, 1.5, 1])

    with col_center:
        # Логотип
        st.markdown(
            '<div style="text-align: center; margin-bottom: 20px;">'
            '<img src="https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png" width="180">'
            '</div>',
            unsafe_allow_html=True,
        )
        
        st.markdown("<h3 style='text-align: center; color: #333; margin-bottom: 5px;'>Welcome to Virshi</h3>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: center; color: #666; margin-bottom: 30px;'>Sign in to manage your AI visibility</p>", unsafe_allow_html=True)

        # Вкладки Вхід / Реєстрація
        tab_login, tab_register = st.tabs(["🔑 Вхід", "📝 Реєстрація"])

        # --- ВКЛАДКА ВХОДУ ---
        with tab_login:
            with st.form("login_form"):
                email = st.text_input("Email", placeholder="name@company.com")
                password = st.text_input("Пароль", type="password", placeholder="••••••••")
                
                st.write("") # Відступ
                
                submit = st.form_submit_button("Увійти", use_container_width=True)
                
                if submit:
                    if not email or not password:
                        st.warning("Будь ласка, заповніть всі поля.")
                    else:
                        # Виклик вашої реальної функції
                        if 'login_user' in globals():
                            login_user(email, password)
                        else:
                            st.error("Функція входу не знайдена.")

        # --- ВКЛАДКА РЕЄСТРАЦІЇ ---
        with tab_register:
            with st.form("register_form"):
                c1, c2 = st.columns(2)
                with c1:
                    first_name = st.text_input("Ім'я", placeholder="Іван")
                with c2:
                    last_name = st.text_input("Прізвище", placeholder="Петренко")
                
                new_email = st.text_input("Email", placeholder="name@company.com")
                new_password = st.text_input("Пароль", type="password", placeholder="••••••••", help="Мін. 6 символів")
                
                st.write("") # Відступ
                
                submit_reg = st.form_submit_button("Створити акаунт", use_container_width=True)
                
                if submit_reg:
                    if not new_email or not new_password or not first_name:
                        st.warning("Будь ласка, заповніть обов'язкові поля.")
                    elif len(new_password) < 6:
                        st.warning("Пароль має містити щонайменше 6 символів.")
                    else:
                        # Виклик вашої реальної функції
                        if 'register_user' in globals():
                            register_user(new_email, new_password, first_name, last_name)
                        else:
                            st.error("Функція реєстрації не знайдена.")


def show_admin_page():
    """
    Адмін-панель (CRM).
    ВЕРСІЯ: PROJECT NAME / BRAND NAME.
    Відображення: "Назва проекту / Назва бренду" у списку.
    """
    import pandas as pd
    import streamlit as st
    import numpy as np
    import time
    import plotly.express as px

    # --- 0. ПІДКЛЮЧЕННЯ ---
    if 'supabase' not in globals():
        if 'supabase' in st.session_state:
            supabase = st.session_state['supabase']
        else:
            st.error("🚨 Помилка: Змінна 'supabase' не знайдена.")
            return
    else:
        supabase = globals()['supabase']

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
            
            if "my_projects" in st.session_state: del st.session_state["my_projects"]
            if "all_projects_admin" in st.session_state: del st.session_state["all_projects_admin"]
            
            st.toast(f"✅ Оновлено: {field} -> {value}")
            time.sleep(0.5)
        except Exception as e:
            st.error(f"Помилка оновлення: {e}")

    st.title("🛡️ Admin Panel (CRM)")

    # --- 1. ОТРИМАННЯ ДАНИХ ---
    try:
        # Отримуємо проекти
        projects_resp = supabase.table("projects").select("*").execute()
        projects_data = projects_resp.data if projects_resp.data else []

        # Отримуємо кількість запитів для статистики
        kws_resp = supabase.table("keywords").select("project_id").execute()
        kws_df = pd.DataFrame(kws_resp.data) if kws_resp.data else pd.DataFrame()
        kw_counts = kws_df['project_id'].value_counts().to_dict() if not kws_df.empty else {}

        # Отримуємо користувачів
        users_resp = supabase.table("profiles").select("*").execute()
        users_data = users_resp.data if users_resp.data else []
        
        # Мапа користувачів для швидкого пошуку
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
    tab_list, tab_users = st.tabs(["📂 Список проектів", "👥 Користувачі & Права"])

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
                
                p_int = p.get('project_name') or ""
                p_brand = p.get('brand_name') or ""
                p_domain = p.get('domain') or ""
                p_id_str = str(p.get('id', ''))
                
                # Пошук по всіх полях
                search_text = f"{p_int} {p_brand} {p_domain} {p_id_str} {owner['full_name']} {owner['email']}".lower()
                
                if search_query and search_query.lower() not in search_text: continue
                if status_filter and p.get('status', 'trial') not in status_filter: continue
                
                filtered_projects.append(p)

            reverse_sort = True if sort_order == "Найновіші" else False
            filtered_projects.sort(key=lambda x: x.get('created_at', ''), reverse=reverse_sort)

        # Header
        h0, h1, h_dash, h2, h3, h_cnt, h4, h5 = st.columns([0.3, 2.5, 0.4, 1.3, 1.2, 0.7, 0.9, 0.5])
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
            
            # 🔥 ФОРМУВАННЯ НАЗВИ: "Project Name / Brand Name"
            p_internal = p.get('project_name')
            p_brand = p.get('brand_name')
            domain = p.get('domain', '')
            
            if p_internal and p_brand:
                # Якщо вони однакові, показуємо один раз
                if p_internal.strip() == p_brand.strip():
                    clean_name = p_internal
                else:
                    clean_name = f"{p_internal} / {p_brand}"
            elif p_internal:
                clean_name = p_internal
            elif p_brand:
                clean_name = p_brand
            else:
                # Якщо назв немає взагалі, беремо домен або заглушку
                clean_name = domain.replace('https://', '').replace('www.', '').split('/')[0] if domain else "Без назви"

            # ЛОГОТИП
            logo_url = None
            backup_logo_url = None
            if domain:
                clean_d = domain.lower().replace('https://', '').replace('http://', '').replace('www.', '')
                if '/' in clean_d: clean_d = clean_d.split('/')[0]
                logo_url = f"https://cdn.brandfetch.io/{clean_d}"
                backup_logo_url = f"https://www.google.com/s2/favicons?domain={clean_d}&sz=64"

            k_count = kw_counts.get(p_id, 0)

            with st.container():
                c0, c1, c_dash, c2, c3, c_cnt, c4, c5 = st.columns([0.3, 2.5, 0.4, 1.3, 1.2, 0.7, 0.9, 0.5])

                with c0: st.caption(f"{idx}")

                with c1:
                    if logo_url:
                        sub_c1, sub_c2 = st.columns([0.15, 0.85])
                        with sub_c1:
                            img_html = f'<img src="{logo_url}" style="width: 30px; border-radius: 4px; pointer-events: none;" onerror="this.onerror=null; this.src=\'{backup_logo_url}\';">'
                            st.markdown(img_html, unsafe_allow_html=True)
                        with sub_c2:
                            st.markdown(f"**{clean_name}**")
                    else:
                        st.markdown(f"**{clean_name}**")
                    
                    st.caption(f"ID: `{p_id}`")
                    if domain: st.caption(f"🌐 {domain}")
                    st.caption(f"👤 {owner_info['full_name']} | {owner_info['email']}")

                with c_dash:
                    if st.button("↗️", key=f"goto_{p_id}", help="Відкрити дашборд"):
                        st.session_state["current_project"] = p
                        st.session_state["force_redirect_to"] = "Дашборд"
                        st.session_state["menu_id_counter"] = st.session_state.get("menu_id_counter", 0) + 1
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
    # TAB 2: КОРИСТУВАЧІ ТА ПРАВА
    # ========================================================
    with tab_users:
        
        # --- БЛОК 1: ТАБЛИЦЯ КОРИСТУВАЧІВ ---
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
            else:
                st.warning("Користувачів не знайдено.")
        else:
            st.warning("База користувачів пуста.")

        st.divider()

        # --- БЛОК 2: ПРИЗНАЧЕННЯ ПРОЕКТІВ ---
        with st.expander("🛠️ Призначити проект користувачу (зміна власника)", expanded=False):
            st.info("Тут ви можете передати існуючий проект іншому користувачу.")
            
            c_asn_1, c_asn_2, c_asn_3 = st.columns([1.5, 1.5, 1])
            
            # 1. Вибір користувача
            user_options = {f"{u['email']} ({u.get('first_name','')} {u.get('last_name','')})": u['id'] for u in users_data}
            
            with c_asn_1:
                selected_user_key = st.selectbox("1. Оберіть нового власника", options=list(user_options.keys()))
            
            # 2. Вибір проекту
            proj_options = {}
            for p in projects_data:
                owner_id = p.get('user_id')
                owner_email = user_map.get(owner_id, {}).get('email', 'Unknown')
                label = f"{p.get('brand_name', 'No Name')} (Власник: {owner_email})"
                proj_options[label] = p['id']
                
            with c_asn_2:
                selected_proj_key = st.selectbox("2. Оберіть проект для передачі", options=list(proj_options.keys()))
            
            with c_asn_3:
                st.write("")
                st.write("")
                if st.button("🔄 Призначити", type="primary", use_container_width=True):
                    if selected_user_key and selected_proj_key:
                        target_user_id = user_options[selected_user_key]
                        target_proj_id = proj_options[selected_proj_key]
                        
                        try:
                            supabase.table("projects").update({"user_id": target_user_id}).eq("id", target_proj_id).execute()
                            st.success(f"Проект успішно передано користувачу {selected_user_key}!")
                            time.sleep(1.5)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Помилка при передачі: {e}")
                    else:
                        st.warning("Оберіть користувача та проект.")

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

def show_chat_page():
    """
    Сторінка AI-асистента (GPT-Visibility).
    Дизайн: Картковий стиль (Card UI) з кастомними бульбашками повідомлень.
    Логіка: Webhook n8n + Context (Sources, Brand, User).
    """
    import requests
    import streamlit as st
    import time

    # --- 1. КОНФІГУРАЦІЯ ---
    if 'N8N_CHAT_WEBHOOK' not in globals():
        target_url = st.secrets.get("N8N_CHAT_WEBHOOK", "")
        if not target_url:
            st.error("🚨 Не задано посилання N8N_CHAT_WEBHOOK.")
            return
    else:
        target_url = N8N_CHAT_WEBHOOK

    # Підключення до бази
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

    # --- 2. CSS СТИЛІЗАЦІЯ (ДИЗАЙН ЗІ СКРІНШОТУ) ---
    st.markdown("""
    <style>
        /* Основний контейнер (Картка) */
        .chat-card-container {
            background-color: white;
            border: 1px solid #e0e0e0;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.05);
            margin-bottom: 100px; /* Місце для інпуту знизу */
        }
        
        /* Заголовок картки */
        .chat-card-header {
            font-family: 'Montserrat', sans-serif;
            font-size: 16px;
            font-weight: 700;
            color: #111;
            padding-bottom: 15px;
            border-bottom: 1px solid #f0f0f0;
            margin-bottom: 20px;
        }

        /* Повідомлення AI (Ліворуч, біле з рамкою) */
        .msg-container-ai {
            display: flex;
            justify-content: flex-start;
            margin-bottom: 15px;
            align-items: flex-start;
        }
        .avatar-ai {
            width: 35px;
            height: 35px;
            background-color: #F3F4F6;
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            margin-right: 10px;
            font-size: 20px;
            flex-shrink: 0;
        }
        .bubble-ai {
            background-color: #ffffff;
            border: 1px solid #6c5ce7; /* Фіолетова рамка як на скріншоті */
            color: #333;
            padding: 12px 16px;
            border-radius: 12px;
            border-top-left-radius: 2px; /* Гострий кут до аватара */
            max-width: 80%;
            font-size: 14px;
            line-height: 1.5;
            box-shadow: 0 2px 4px rgba(0,0,0,0.03);
        }
        .ai-label {
            font-size: 11px;
            font-weight: 700;
            color: #333;
            margin-bottom: 4px;
            display: block;
        }

        /* Повідомлення Користувача (Праворуч, фіолетове) */
        .msg-container-user {
            display: flex;
            justify-content: flex-end;
            margin-bottom: 15px;
        }
        .bubble-user {
            background-color: #6c5ce7; /* Primary Purple */
            color: white;
            padding: 12px 16px;
            border-radius: 12px;
            border-bottom-right-radius: 2px;
            max-width: 80%;
            font-size: 14px;
            line-height: 1.5;
            box-shadow: 0 2px 5px rgba(108, 92, 231, 0.2);
            text-align: left;
        }
        
        /* Приховати стандартні елементи Streamlit, що заважають дизайну */
        .stChatMessage { display: none !important; } 
    </style>
    """, unsafe_allow_html=True)

    # --- 3. ЛОГІКА ДАНИХ ---
    user = st.session_state.get("user")
    role = st.session_state.get("role", "user") 
    proj = st.session_state.get("current_project", {})
    
    if not proj:
        st.info("⚠️ Спочатку оберіть проект у меню зліва.")
        return

    # Ім'я користувача
    user_name = "Користувач"
    if user:
        meta = getattr(user, "user_metadata", {})
        user_name = meta.get("full_name") or meta.get("name") or user.email.split("@")[0]

    # Офіційні джерела (Whitelist)
    official_sources_list = []
    try:
        assets_resp = supabase.table("official_assets")\
            .select("domain_or_url")\
            .eq("project_id", proj.get("id"))\
            .execute()
        if assets_resp.data:
            official_sources_list = [item["domain_or_url"] for item in assets_resp.data]
    except Exception:
        official_sources_list = []

    # Ініціалізація історії
    if "chat_messages" not in st.session_state:
        brand_name = proj.get('brand_name', 'Brand')
        welcome_text = f"Based on the latest analysis, **{brand_name}**'s presence has improved. I'm ready to help you with visibility insights."
        st.session_state["chat_messages"] = [
            {"role": "assistant", "content": welcome_text}
        ]

    # --- 4. ВІДОБРАЖЕННЯ ІНТЕРФЕЙСУ (КАРТКА) ---
    
    # Заголовок сторінки (як в дизайні)
    st.markdown("### 🤖 AI Visibility Assistant")

    # Контейнер-картка
    chat_container = st.container()
    
    with chat_container:
        # Відкриваємо div картки
        st.markdown(f"""
        <div class="chat-card-container">
            <div class="chat-card-header">
                Project: {proj.get('brand_name', 'Unknown')} - AI Chat Assistant (GPT-Visibility)
            </div>
        """, unsafe_allow_html=True)

        # Рендеринг повідомлень (HTML Loop)
        for msg in st.session_state["chat_messages"]:
            content = msg["content"]
            
            if msg["role"] == "assistant":
                st.markdown(f"""
                <div class="msg-container-ai">
                    <div class="avatar-ai">🤖</div>
                    <div class="bubble-ai">
                        <span class="ai-label">AI Assistant</span>
                        {content}
                    </div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div class="msg-container-user">
                    <div class="bubble-user">
                        {content}
                    </div>
                </div>
                """, unsafe_allow_html=True)
        
        # Закриваємо div картки
        st.markdown("</div>", unsafe_allow_html=True)

    # --- 5. ВВЕДЕННЯ ТА ОБРОБКА ---
    
    if prompt := st.chat_input("Ask GPT-Visibility about your brand's AI presence..."):
        
        # 1. Додаємо питання користувача в історію
        st.session_state["chat_messages"].append({"role": "user", "content": prompt})
        st.rerun() # Оновлюємо, щоб показати повідомлення користувача одразу

    # Логіка відповіді (спрацьовує після rerun, якщо останнє повідомлення - від user)
    if st.session_state["chat_messages"] and st.session_state["chat_messages"][-1]["role"] == "user":
        
        last_user_msg = st.session_state["chat_messages"][-1]["content"]
        
        # Показуємо спінер над інпутом (або під карткою)
        with st.spinner("AI Assistant is typing..."):
            try:
                # Payload
                payload = {
                    "query": last_user_msg,
                    "user_id": user.id if user else "guest",
                    "user_email": user.email if user else None,
                    "user_name": user_name,
                    "role": role,
                    "project_id": proj.get("id"),
                    "project_name": proj.get("brand_name"),
                    "target_brand": proj.get("brand_name"),
                    "domain": proj.get("domain"),
                    "status": proj.get("status"),
                    "official_sources": official_sources_list
                }

                response = requests.post(
                    target_url, 
                    json=payload, 
                    headers=headers, 
                    timeout=240
                )

                if response.status_code == 200:
                    data = response.json()
                    bot_reply = data.get("output") or data.get("answer") or data.get("text")
                    
                    if isinstance(bot_reply, dict):
                        bot_reply = str(bot_reply)
                    
                    if not bot_reply:
                        bot_reply = "⚠️ I received an empty response from the AI."
                        
                elif response.status_code == 403:
                    bot_reply = "⛔ Error 403: Access denied. Check API keys."
                elif response.status_code == 404:
                    bot_reply = "⚠️ Error 404: Endpoint not found."
                else:
                    bot_reply = f"⚠️ Server Error: {response.status_code}"

            except Exception as e:
                bot_reply = f"⚠️ Connection Error: {e}"

            # Додаємо відповідь бота в історію
            st.session_state["chat_messages"].append({"role": "assistant", "content": bot_reply})
            st.rerun()
        
            
def main():
    # 1. Ініціалізація та перевірка сесії
    if 'check_session' in globals():
        check_session()

    # 2. ПЕРЕВІРКА АВТОРИЗАЦІЇ
    if not st.session_state.get("user"):
        if 'show_auth_page' in globals():
            show_auth_page()
        else:
            st.error("Функція авторизації не знайдена.")
        return  # Зупиняємо виконання, якщо немає юзера

    # 3. ОТРИМАННЯ ДАНИХ ПРОЕКТУ (Спроба знайти існуючий)
    if not st.session_state.get("current_project"):
        try:
            user_id = st.session_state["user"].id
            # Отримуємо клієнт Supabase
            sb_client = globals().get('supabase') or st.session_state.get('supabase')
            
            if sb_client:
                resp = sb_client.table("projects").select("*").eq("user_id", user_id).execute()
                
                if resp.data:
                    # Якщо проекти є -> беремо перший і зберігаємо в сесію
                    st.session_state["current_project"] = resp.data[0]
                    st.rerun() # Перезавантажуємо сторінку, щоб показати Дашборд
        except Exception as e:
            # st.error(f"Error fetching project: {e}")
            pass

    # 4. ЛОГІКА ДЛЯ НОВИХ КОРИСТУВАЧІВ (Якщо проекту все ще немає)
    user_role = st.session_state.get("role", "user")
    
    # Якщо проекту немає і це не адмін -> ПРИМУСОВО показуємо сторінку створення
    if st.session_state.get("current_project") is None and user_role not in ["admin", "super_admin"]:
        
        # 1. Малюємо сайдбар (щоб можна було вийти)
        if 'sidebar_menu' in globals():
            # Тут ми можемо викликати спрощену версію меню або повну
            # Але оскільки current_project = None, меню покаже "Оберіть проект" або пусте поле
            with st.sidebar:
                st.image("https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png", width=150)
                st.markdown("---")
                if st.button("🚪 Вийти з акаунту", use_container_width=True):
                    if 'logout' in globals(): logout()
                    else: st.session_state.clear(); st.rerun()
        
        # 2. Показуємо сторінку проектів (вона сама відкриє вкладку "Створити")
        if 'show_my_projects_page' in globals():
            show_my_projects_page()
        else:
            st.warning("Сторінка проектів не знайдена.")
        
        return # <--- Зупиняємо скрипт тут! Дашборд нижче не виконається.

    # =========================================================
    # 5. ОСНОВНИЙ ДОДАТОК (Тільки якщо є User і Project)
    # =========================================================
    
    # 1. Меню навігації
    page = "Дашборд"
    if 'sidebar_menu' in globals():
        page = sidebar_menu()

    # 2. Роутинг сторінок
    if page == "Дашборд":
        if 'show_dashboard' in globals(): show_dashboard()
    
    elif page == "Мої проекти":    
        if 'show_my_projects_page' in globals(): show_my_projects_page()
        
    elif page == "Перелік запитів":
        if 'show_keywords_page' in globals(): show_keywords_page()
        
    elif page == "Джерела":
        if 'show_sources_page' in globals(): show_sources_page()
        
    elif page == "Конкуренти":
        if 'show_competitors_page' in globals(): show_competitors_page()
        else: st.info("Розділ 'Конкуренти' в розробці.")
            
    elif page == "Рекомендації":
        if 'show_recommendations_page' in globals(): show_recommendations_page()

    elif page == "Історія сканувань":
        if 'show_history_page' in globals(): show_history_page()
        
    elif page == "Звіти":
        if 'show_reports_page' in globals(): show_reports_page()
        
    elif page == "FAQ":
        if 'show_faq_page' in globals(): show_faq_page()

    elif page == "GPT-Visibility":
        if 'show_chat_page' in globals(): show_chat_page()
        
    elif page == "Адмін":
        if user_role in ["admin", "super_admin"]:
            if 'show_admin_page' in globals(): show_admin_page()
        else:
            st.error("Доступ заборонено.")

if __name__ == "__main__":
    main()
