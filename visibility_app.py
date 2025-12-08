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
N8N_ANALYZE_URL = "https://virshi.app.n8n.cloud/webhook-test/webhook/run-analysis"
N8N_RECO_URL = "https://virshi.app.n8n.cloud/webhook/recommendations"  # за потреби заміниш

# Custom CSS
st.markdown(
    """
<style>
    .stApp { background-color: #F4F6F9; }
    section[data-testid="stSidebar"] { background-color: #FFFFFF; border-right: 1px solid #E0E0E0; }
    .sidebar-logo-container { display: flex; justify-content: center; margin-bottom: 10px; }
    .sidebar-logo-container img { width: 140px; }

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

    /* Додати до існуючих стилів */
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
    .metric-card-small {
        background-color: #F0F2F6;
        border-radius: 6px;
        padding: 10px;
        text-align: center;
    }
    .metric-value {
        font-size: 18px;
        font-weight: bold;
        color: #8041F6;
    }
    .metric-label {
        font-size: 12px;
        color: #666;
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
    Відправляє запит на n8n.
    """
    try:
        user_email = st.session_state["user"].email if st.session_state.get("user") else None
        
        # Якщо keywords це один рядок, робимо з нього список
        if isinstance(keywords, str):
            keywords = [keywords]

        payload = {
            "project_id": project_id,
            "keywords": keywords, # Передаємо масив
            "brand_name": brand_name,
            "user_email": user_email,
            "models": models or ["perplexity"], # За замовчуванням perplexity
        }
        
        # Збільшуємо таймаут, бо n8n може думати пару секунд
        response = requests.post(N8N_ANALYZE_URL, json=payload, timeout=5)
        
        if response.status_code == 200:
            return True
        else:
            st.error(f"N8N Error: {response.text}")
            return False
            
    except Exception as e:
        st.error(f"Помилка з'єднання з n8n: {e}")
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
    try:
        supabase.auth.sign_out()
    except Exception:
        pass
    cookie_manager.delete("virshi_auth_token")
    st.session_state["user"] = None
    st.session_state["current_project"] = None
    st.session_state["focus_keyword_id"] = None
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
    st.markdown("## 🚀 Налаштування Проекту")

    with st.container(border=True):
        step = st.session_state.get("onboarding_step", 2)

        # STEP 2 – дані про бренд
        if step == 2:
            st.subheader("Крок 1: Введіть дані про ваш бренд")

            brand = st.text_input("Назва бренду")
            domain = st.text_input("Домен (офіційний сайт)")
            industry = st.text_input("Галузь бренду / ніша")
            products = st.text_area(
                "Продукти / Послуги (перелічіть через кому або у стовпчик)"
            )

            if st.button("Згенерувати запити"):
                if brand and domain and industry and products:
                    st.session_state["temp_brand"] = brand
                    st.session_state["temp_domain"] = domain
                    st.session_state["temp_industry"] = industry
                    st.session_state["temp_products"] = products

                    with st.spinner("Генеруємо релевантні запити через n8n AI Agent..."):
                        prompts = n8n_generate_prompts(brand, domain, industry, products)
                        if prompts and len(prompts) > 0:
                            st.session_state["generated_prompts"] = prompts
                            st.session_state["onboarding_step"] = 3
                            st.rerun()
                        else:
                            st.error("AI не повернув результатів. Спробуйте ще раз.")
                else:
                    st.warning("Будь ласка, заповніть всі 4 поля.")

        # STEP 3 – вибір 5 запитів
        elif step == 3:
            st.subheader("Крок 2: Оберіть 5 пріоритетних запитів")
            st.write(
                f"Оберіть 5 пріоритетних запитів для **{st.session_state['temp_brand']}**:"
            )

            opts = st.session_state.get("generated_prompts", [])
            selected = st.multiselect(
                "Список запитів:",
                opts,
                default=opts[:5] if len(opts) >= 5 else opts,
            )
            st.caption(f"Обрано: {len(selected)} / 5")

            if st.button("Запустити аналіз"):
                if len(selected) == 5:
                    with st.spinner("Створюємо проект та запускаємо аналіз..."):
                        try:
                            user_id = st.session_state["user"].id

                            # 1. Створюємо проект
                            res = (
                                supabase.table("projects")
                                .insert(
                                    {
                                        "user_id": user_id,
                                        "brand_name": st.session_state["temp_brand"],
                                        "domain": st.session_state["temp_domain"],
                                        "industry": st.session_state[
                                            "temp_industry"
                                        ],
                                        "products": st.session_state[
                                            "temp_products"
                                        ],
                                        "status": "trial",
                                    }
                                )
                                .execute()
                            )

                            if not res.data:
                                raise Exception("Project creation failed")

                            proj_data = res.data[0]
                            proj_id = proj_data["id"]

                            # 2. Записуємо ключові слова
                            for kw in selected:
                                supabase.table("keywords").insert(
                                    {
                                        "project_id": proj_id,
                                        "keyword_text": kw,
                                        "type": "ranking",
                                    }
                                ).execute()

                            # 3. Запускаємо аналіз через n8n
                            n8n_trigger_analysis(
                                proj_id, selected, st.session_state["temp_brand"]
                            )

                            # 4. Фінал
                            st.session_state["current_project"] = proj_data
                            st.success(
                                "Проект створено! Аналіз запущено у фоновому режимі."
                            )
                            time.sleep(2)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Системна помилка: {e}")
                else:
                    st.error("Будь ласка, оберіть рівно 5 запитів")


# =========================
# 6. DASHBOARD
# =========================



def show_dashboard():
    proj = st.session_state.get("current_project", {})
    if not proj:
        st.info("Спочатку створіть проект.")
        return

    # Заголовок і фільтр періоду
    c1, c2 = st.columns([3, 1])
    with c1:
        st.title(f"📊 Дашборд: {proj.get('brand_name')}")
        st.caption("Зведена аналітика видимості у LLM (Perplexity, GPT, Gemini)")
    with c2:
        # Поки що логіка фільтрації візуальна, SQL View повертає всі дані
        st.selectbox("Період:", ["Останні 30 днів", "Все"], index=0)
    
    st.markdown("---")

    # 1. ЗАВАНТАЖЕННЯ KPI (З SQL VIEW)
    # Ми звертаємося до віртуальної таблиці, яку створили в SQL
    try:
        stats_resp = supabase.table("project_dashboard_stats").select("*").eq("project_id", proj["id"]).execute()
        if stats_resp.data:
            stats = stats_resp.data[0]
        else:
            stats = {}
    except Exception as e:
        # Якщо View ще не створена або помилка, показуємо нулі, щоб не крашити додаток
        # st.error(f"Помилка KPI: {e}") 
        stats = {}

    # Розпаковка даних (безпечно, з дефолтними нулями)
    sov = stats.get("sov", 0)
    off_src = stats.get("official_source_pct", 0)
    avg_sent = stats.get("avg_sentiment", 0)
    avg_pos = stats.get("avg_position", 0)
    
    # Абсолютні цифри (з JSON поля absolute_counts)
    abs_counts = stats.get("absolute_counts", {})
    total_mentions = abs_counts.get("total_mentions", 0)
    my_mentions = abs_counts.get("my_mentions", 0)

    # 2. ВІДОБРАЖЕННЯ КАРТОК KPI
    k1, k2, k3, k4 = st.columns(4)
    
    with k1:
        with st.container(border=True):
            st.metric(
                "📢 Share of Voice", 
                f"{sov:.1f}%", 
                help=f"Вас згадали {my_mentions} разів із {total_mentions} загальних згадок брендів."
            )
            # Малюємо міні-графік (пончик)
            st.plotly_chart(get_donut_chart(sov, "#8041F6"), use_container_width=True, key="d_sov")

    with k2:
        with st.container(border=True):
            st.metric(
                "🛡️ Official Sources", 
                f"{off_src:.1f}%",
                help="Відсоток посилань, які ведуть саме на ваші сайти (з Whitelist)"
            )
            st.plotly_chart(get_donut_chart(off_src, "#00C896"), use_container_width=True, key="d_off")

    with k3:
        with st.container(border=True):
            st.metric(
                "❤️ Sentiment", 
                f"{avg_sent:.0f}/100",
                help="Середня тональність (0-негатив, 100-позитив)"
            )
            # Прогресбар тональності
            st.progress(int(avg_sent))

    with k4:
        with st.container(border=True):
            # Якщо позиція 0, значить нас ніде не знайшли, пишемо прочерк
            pos_display = f"#{avg_pos:.1f}" if avg_pos > 0 else "-"
            st.metric(
                "🏆 Avg Position", 
                pos_display,
                help="Середня позиція у списках рекомендацій (де бренд був знайдений)"
            )
            if avg_pos > 0:
                # Чим менше число (ближче до 1), тим краще, тому інвертуємо прогресбар
                # Якщо позиція 1 -> 100%, якщо позиція 10 -> 0%
                val = max(0, 100 - (int(avg_pos) - 1) * 10)
                st.progress(val)
            else:
                st.caption("Немає даних")

    # 3. ГРАФІК ДИНАМІКИ (З SQL VIEW 2)
    c_chart, c_list = st.columns([2, 1])

    with c_chart:
        st.subheader("📈 Динаміка Настрою (Sentiment)")
        try:
            trends_resp = supabase.table("daily_sentiment_trends").select("*").eq("project_id", proj["id"]).execute()
            trends_data = trends_resp.data
        except:
            trends_data = []

        if trends_data:
            df_trends = pd.DataFrame(trends_data)
            
            fig = px.line(
                df_trends, 
                x="scan_date", 
                y="avg_sentiment",
                markers=True,
                title="Як змінювалася тональність згадок",
                labels={"scan_date": "Дата", "avg_sentiment": "Бали (0-100)"}
            )
            # Стилізація графіка під бренд
            fig.update_traces(line_color='#8041F6', line_width=3)
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", 
                plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=20, r=20, t=40, b=20),
                height=350
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            # Заглушка, якщо даних ще немає
            st.info("Графік будується... Запустіть більше сканувань у різні дні.")

    # 4. СПИСОК ОСТАННІХ ЗАПИТІВ (Права колонка)
    with c_list:
        st.subheader("🔥 Активні запити")
        try:
            # Беремо останні 5 запитів
            recent_kws = supabase.table("keywords").select("*").eq("project_id", proj["id"]).order("id", desc=True).limit(5).execute().data
        except:
            recent_kws = []

        if recent_kws:
            for k in recent_kws:
                with st.container(border=True):
                    col_txt, col_btn = st.columns([3, 1])
                    col_txt.markdown(f"**{k['keyword_text']}**")
                    # Кнопка для швидкого переходу до аналізу
                    if col_btn.button("🔍", key=f"dash_go_{k['id']}"):
                        st.session_state["focus_keyword_id"] = k["id"]
                        # Примусово перемикаємо сторінку (якщо використовується option_menu)
                        st.session_state["force_page"] = "Перелік запитів" 
                        st.rerun()
        else:
            st.caption("Тут з'являться ваші останні запити.")
            if st.button("Додати перший запит"):
                st.session_state["force_page"] = "Перелік запитів"
                st.rerun()

# =========================
# 7. КЕРУВАННЯ ЗАПИТАМИ
# =========================

def show_keyword_details(kw_id):
    """
    Відображає детальну аналітику по конкретному запиту з історією та вкладками моделей.
    """
    import pandas as pd
    import streamlit as st
    
    # --- 0. ПІДКЛЮЧЕННЯ ДО БАЗИ (Safety Check) ---
    if 'supabase' not in globals():
        if 'supabase' in st.session_state:
            supabase = st.session_state['supabase']
        else:
            st.error("🚨 Помилка: Змінна 'supabase' не знайдена.")
            return
    else:
        supabase = globals()['supabase']

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

    # --- 2. HEADER ТА НАВІГАЦІЯ ---
    col_back, col_title = st.columns([1, 5])
    with col_back:
        if st.button("⬅ Назад", key="back_main"):
            st.session_state["focus_keyword_id"] = None
            st.rerun()
    
    with col_title:
        st.title(f"🔍 {keyword_text}")

    # --- 3. БЛОК УПРАВЛІННЯ (РЕДАГУВАННЯ ТА СКАНУВАННЯ) ---
    with st.expander("⚙️ Налаштування та Нове сканування", expanded=False):
        c1, c2 = st.columns(2)
        
        # А: Редагування тексту
        with c1:
            st.subheader("✏️ Редагувати запит")
            new_text = st.text_input("Текст запиту", value=keyword_text, key="edit_kw_input")
            if st.button("💾 Зберегти зміни", key="save_kw_btn"):
                if new_text and new_text != keyword_text:
                    supabase.table("keywords").update({"keyword_text": new_text}).eq("id", kw_id).execute()
                    st.success("Збережено!")
                    st.rerun()

        # Б: Запуск сканування
        with c2:
            st.subheader("🚀 Запустити тест")
            available_models = ["perplexity", "gpt-4o", "gemini-1.5-pro"]
            selected_models = st.multiselect(
                "Оберіть моделі для тесту:", 
                available_models, 
                default=["perplexity"],
                key="rescan_models"
            )
            
            if st.button("▶️ Сканувати зараз", key="rescan_btn"):
                if selected_models:
                    proj = st.session_state.get("current_project", {})
                    brand_name = proj.get("brand_name", "MyBrand")
                    
                    with st.spinner(f"Запускаємо {', '.join(selected_models)}..."):
                        # Виклик існуючої функції n8n_trigger_analysis
                        success = n8n_trigger_analysis(project_id, [new_text], brand_name, models=selected_models)
                        if success:
                            st.success("Задачу відправлено! Оновіть сторінку за хвилину.")
                else:
                    st.warning("Оберіть хоча б одну модель.")

    st.divider()

    # --- 4. ОТРИМАННЯ ВСІХ СКАНУВАНЬ ---
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
        st.info("📭 Для цього запиту ще немає результатів. Скористайтеся формою вище, щоб запустити сканування.")
        return

    # --- 5. ВКЛАДКИ ПО МОДЕЛЯХ (TABS) ---
    # Визначаємо, які моделі взагалі є в базі + стандартні
    # (Щоб завжди були вкладки, навіть якщо даних ще немає)
    model_tabs_names = ["perplexity", "gpt-4o", "gemini-1.5-pro"]
    tabs = st.tabs([m.upper() for m in model_tabs_names])

    for tab, model_key in zip(tabs, model_tabs_names):
        with tab:
            # Фільтруємо сканування тільки для цієї моделі
            # (Ми шукаємо входження, бо іноді provider може бути 'perplexity/sonar' тощо)
            model_scans = [s for s in scans_data if model_key in (s.get("provider") or "").lower()]
            
            if not model_scans:
                st.write(f"📉 Даних від **{model_key}** ще немає.")
                continue

            # --- ВИПАДАЮЧИЙ СПИСОК ІСТОРІЇ ---
            # Словник: "2023-10-12 14:30" -> scan_object
            history_options = {s["created_at"][:16].replace("T", " "): s for s in model_scans}
            
            selected_time = st.selectbox(
                f"📅 Оберіть дату сканування ({model_key}):", 
                list(history_options.keys()),
                key=f"hist_sel_{model_key}"
            )
            
            # Отримуємо конкретний об'єкт сканування
            current_scan = history_options[selected_time]
            scan_id = current_scan["id"]

            # === ВІДОБРАЖЕННЯ ДАНИХ (Як раніше) ===
            
            # 1. Текст відповіді
            raw_text = current_scan.get("raw_response", "")
            st.markdown("##### 📝 Відповідь ШІ")
            with st.expander("Читати повний текст", expanded=False):
                if raw_text:
                    my_brand = st.session_state.get("current_project", {}).get("brand_name", "")
                    if my_brand:
                        st.markdown(raw_text.replace(my_brand, f"**{my_brand}**"))
                    else:
                        st.markdown(raw_text)
                else:
                    st.caption("Текст відсутній.")

            # 2. Таблиця Брендів
            st.markdown("##### 📊 Знайдені бренди")
            try:
                mentions = (
                    supabase.table("brand_mentions")
                    .select("*")
                    .eq("scan_result_id", scan_id)
                    .order("rank_position", nullsfirst=False)
                    .execute()
                    .data
                )
                if mentions:
                    df_brands = pd.DataFrame(mentions)
                    cols = ["rank_position", "brand_name", "sentiment_score", "mention_count", "is_my_brand"]
                    avail_cols = [c for c in cols if c in df_brands.columns]
                    show_df = df_brands[avail_cols].copy()
                    
                    rename_map = {
                        "rank_position": "Ранг", "brand_name": "Бренд", 
                        "sentiment_score": "Тон", "mention_count": "Згадок", "is_my_brand": "Ми?"
                    }
                    show_df.rename(columns=rename_map, inplace=True)
                    if "Ми?" in show_df.columns:
                        show_df["Ми?"] = show_df["Ми?"].apply(lambda x: "✅" if x else "")
                    
                    st.dataframe(show_df, use_container_width=True, hide_index=True)
                else:
                    st.info("Брендів не знайдено.")
            except Exception as e:
                st.error(f"Помилка брендів: {e}")

            # 3. Таблиця Джерел
            st.markdown("##### 🔗 Джерела")
            try:
                sources = (
                    supabase.table("extracted_sources")
                    .select("*")
                    .eq("scan_result_id", scan_id)
                    .execute()
                    .data
                )
                if sources:
                    df_src = pd.DataFrame(sources)
                    s_cols = ["domain", "url", "is_official"]
                    s_avail = [c for c in s_cols if c in df_src.columns]
                    show_src = df_src[s_avail].copy()
                    
                    show_src.rename(columns={"domain": "Домен", "url": "URL", "is_official": "Оф?"}, inplace=True)
                    if "Оф?" in show_src.columns:
                        show_src["Оф?"] = show_src["Оф?"].apply(lambda x: "✅" if x else "")

                    st.dataframe(
                        show_src, 
                        use_container_width=True, 
                        hide_index=True,
                        column_config={"URL": st.column_config.LinkColumn("URL")}
                    )
                else:
                    st.info("Джерел не знайдено.")
            except Exception as e:
                st.error(f"Помилка джерел: {e}")

def show_keywords_page():
    """
    Головна функція сторінки запитів. Маршрутизує між списком та деталями.
    """
    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект в онбордингу.")
        return

    # Якщо вибрано ID - показуємо деталі
    if st.session_state.get("focus_keyword_id"):
        show_keyword_details(st.session_state["focus_keyword_id"])
        return

    st.title("📋 Перелік запитів")

    # Форма додавання
    with st.expander("➕ Додати новий запит", expanded=False):
        with st.form("add_keyword_form"):
            new_kw = st.text_input("Введіть запит")
            model_choices = ["perplexity", "gpt-4o", "gemini-1.5-pro"]
            selected_models = st.multiselect("Оберіть моделі:", model_choices, default=["perplexity"])
            
            if st.form_submit_button("Додати та Просканувати"):
                if new_kw:
                    try:
                        res = supabase.table("keywords").insert({
                            "project_id": proj["id"], "keyword_text": new_kw, "is_active": True
                        }).execute()
                        if res.data:
                            # Запуск сканування
                            n8n_trigger_analysis(proj["id"], [new_kw], proj.get("brand_name"), models=selected_models)
                            st.success(f"Запит '{new_kw}' додано.")
                            time.sleep(1)
                            st.rerun()
                    except Exception as e:
                        st.error(f"Помилка: {e}")

    st.markdown("---")
    
    # Список запитів
    try:
        keywords = supabase.table("keywords").select("*").eq("project_id", proj["id"]).order("id", desc=True).execute().data
    except:
        keywords = []

    if not keywords:
        st.info("Запити відсутні.")
        return

    col_h1, col_h2, col_h3 = st.columns([3, 1, 1])
    col_h1.markdown("**Запит**")
    col_h2.markdown("**Статус**")
    col_h3.markdown("**Дії**")

    for k in keywords:
        with st.container(border=True):
            c1, c2, c3 = st.columns([3, 1, 1])
            c1.markdown(f"**{k['keyword_text']}**")
            c2.markdown("✅ Active")
            with c3:
                if st.button("🔍 Деталі", key=f"det_{k['id']}"):
                    st.session_state["focus_keyword_id"] = k["id"]
                    st.rerun()
                if st.button("🗑", key=f"del_{k['id']}"):
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
    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект.")
        return

    st.title("📡 Джерела та Репутація")
    
    tab1, tab2 = st.tabs(["🛡️ Мої Активи (Whitelist)", "🌐 Аналіз Ринку"])

    # --- TAB 1: МОЇ ОФІЦІЙНІ ДЖЕРЕЛА ---
    with tab1:
        st.markdown("Додайте сюди ваші офіційні сайти та соцмережі.")
        
        # Форма додавання
        with st.expander("➕ Додати нове джерело", expanded=False):
            with st.form("add_asset_form"):
                c1, c2 = st.columns([2, 1])
                with c1:
                    new_asset = st.text_input("URL або Домен")
                with c2:
                    asset_type = st.selectbox("Тип", ["website", "social", "article", "other"])
                
                if st.form_submit_button("Зберегти"):
                    if new_asset:
                        try:
                            supabase.table("official_assets").insert({
                                "project_id": proj["id"],
                                "domain_or_url": new_asset,
                                "type": asset_type
                            }).execute()
                            st.success(f"Джерело додано.")
                            time.sleep(0.5)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Помилка: {e}")

        st.divider()

        # --- ТАБЛИЦЯ З РЕДАГУВАННЯМ ---
        try:
            assets = supabase.table("official_assets").select("*").eq("project_id", proj["id"]).order("created_at", desc=True).execute().data
        except:
            assets = []

        if assets:
            for asset in assets:
                # Використовуємо контейнер для кожного рядка
                with st.container(border=True):
                    # Якщо натиснуто "Редагувати", показуємо форму
                    if st.session_state.get(f"edit_mode_{asset['id']}", False):
                        c_edit, c_btn = st.columns([4, 1])
                        with c_edit:
                            new_val = st.text_input("URL", value=asset['domain_or_url'], key=f"in_{asset['id']}")
                            new_type = st.selectbox("Тип", ["website", "social", "article", "other"], index=["website", "social", "article", "other"].index(asset['type']), key=f"sel_{asset['id']}")
                        
                        col_save, col_cancel = st.columns(2)
                        if col_save.button("💾 Зберегти", key=f"save_{asset['id']}"):
                            supabase.table("official_assets").update({
                                "domain_or_url": new_val,
                                "type": new_type
                            }).eq("id", asset['id']).execute()
                            st.session_state[f"edit_mode_{asset['id']}"] = False
                            st.rerun()
                            
                        if col_cancel.button("Скасувати", key=f"cancel_{asset['id']}"):
                            st.session_state[f"edit_mode_{asset['id']}"] = False
                            st.rerun()
                            
                    else:
                        # Режим перегляду
                        c1, c2, c3, c4 = st.columns([3, 1, 0.5, 0.5])
                        with c1:
                            st.markdown(f"**{asset['domain_or_url']}**")
                        with c2:
                            st.caption(asset['type'].upper())
                        with c3:
                            if st.button("✏️", key=f"edit_btn_{asset['id']}"):
                                st.session_state[f"edit_mode_{asset['id']}"] = True
                                st.rerun()
                        with c4:
                            if st.button("🗑", key=f"del_as_{asset['id']}"):
                                supabase.table("official_assets").delete().eq("id", asset['id']).execute()
                                st.rerun()
        else:
            st.info("Список порожній.")

    # --- TAB 2: АНАЛІЗ РИНКУ ---
    with tab2:
        # (Код для аналізу ринку залишається без змін з попереднього разу)
        # Просто додайте try/except блок, як було раніше
        pass 
        # ... (код з попередньої відповіді)
def show_competitors_page():
    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект.")
        return

    st.title("⚔️ Аналіз Конкурентів")
    st.caption("Кого ШІ рекомендує поруч із вами? Порівняння видимості та репутації.")

    # 1. Завантаження даних з SQL View
    try:
        data = (
            supabase.table("competitor_stats")
            .select("*")
            .eq("project_id", proj["id"])
            .execute()
            .data
        )
    except Exception as e:
        st.error(f"Помилка завантаження даних: {e}")
        data = []

    if not data:
        st.info("Даних ще недостатньо. Запустіть кілька сканувань у 'Перелік запитів', щоб ШІ знайшов конкурентів.")
        return

    # Перетворюємо в DataFrame для зручної роботи
    df = pd.DataFrame(data)

    # 2. Метрики лідера (Хто головний конкурент?)
    # Виключаємо наш бренд, щоб знайти реального ворога
    competitors_only = df[df['is_my_brand'] == False]
    
    if not competitors_only.empty:
        # Сортуємо за кількістю згадок
        top_rival = competitors_only.sort_values(by="total_mentions", ascending=False).iloc[0]
        
        # Знаходимо нас
        my_brand = df[df['is_my_brand'] == True]
        my_mentions = my_brand.iloc[0]['total_mentions'] if not my_brand.empty else 0
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Головний конкурент", top_rival['brand_name'])
        c2.metric("Його згадок", top_rival['total_mentions'], delta=int(top_rival['total_mentions'] - my_mentions), delta_color="inverse")
        c3.metric("Його тональність", f"{int(top_rival['avg_sentiment'])}/100")
    
    st.divider()

    # 3. Графік 1: КАРТА РЕПУТАЦІЇ (Scatter Plot)
    # Це найкрутіший графік для GEO.
    st.subheader("🗺️ Карта Репутації (Magic Quadrant)")
    st.caption("Чим вище — тим краще відгукуються. Чим правіше — тим частіше згадують.")

    if not df.empty:
        # Додаємо колонку кольору: Мій бренд = Фіолетовий, Інші = Сірий
        df['Color'] = df['is_my_brand'].apply(lambda x: 'Мій Бренд' if x else 'Конкурент')
        df['Size'] = df['total_mentions'] * 2 # Розмір бульбашки

        fig = px.scatter(
            df,
            x="total_mentions",
            y="avg_sentiment",
            size="Size",
            color="Color",
            text="brand_name",
            color_discrete_map={'Мій Бренд': '#8041F6', 'Конкурент': '#9EA0A5'},
            hover_data=["avg_rank"],
            height=500
        )
        # Налаштування вигляду
        fig.update_traces(textposition='top center')
        fig.update_layout(
            xaxis_title="Кількість згадок (Видимість)",
            yaxis_title="Середня тональність (Якість)",
            yaxis_range=[0, 105], # Щоб графік завжди був від 0 до 100
            showlegend=True
        )
        # Малюємо лінії середини
        fig.add_hline(y=50, line_dash="dot", line_color="lightgray")
        
        st.plotly_chart(fig, use_container_width=True)

    # 4. Графік 2: Рейтинг за часткою голосу (Bar Chart)
    st.subheader("📊 Рейтинг за часткою голосу (Share of Voice)")
    
    if not df.empty:
        # Сортуємо для краси
        df_sorted = df.sort_values(by="total_mentions", ascending=True) # Ascending для горизонтального бару
        
        fig_bar = px.bar(
            df_sorted,
            x="total_mentions",
            y="brand_name",
            orientation='h',
            text="total_mentions",
            color="is_my_brand",
            color_discrete_map={True: '#8041F6', False: '#D1D1D6'}
        )
        fig_bar.update_layout(showlegend=False, xaxis_title="Кількість згадок", yaxis_title="")
        st.plotly_chart(fig_bar, use_container_width=True)

    # 5. Детальна таблиця
    with st.expander("📋 Дивитися детальні дані таблицею"):
        # Готуємо красиву таблицю
        display_df = df[['brand_name', 'total_mentions', 'avg_sentiment', 'avg_rank', 'is_my_brand']].copy()
        display_df.columns = ['Бренд', 'Згадок', 'Тональність', 'Сер. Позиція', 'Це ми?']
        
        # Форматуємо числа
        display_df['Тональність'] = display_df['Тональність'].astype(int)
        display_df['Сер. Позиція'] = display_df['Сер. Позиція'].apply(lambda x: f"#{x:.1f}" if x else "-")
        display_df['Це ми?'] = display_df['Це ми?'].apply(lambda x: "✅" if x else "")
        
        # Сортуємо
        display_df = display_df.sort_values(by="Згадок", ascending=False)
        
        st.dataframe(
            display_df, 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "Згадок": st.column_config.ProgressColumn(
                    "Частка",
                    format="%d",
                    min_value=0,
                    max_value=int(df['total_mentions'].max())
                )
            }
        )

def show_chat_page():
    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект.")
        return

    st.title(f"🤖 Virshi AI: Асистент для {proj.get('brand_name')}")
    st.caption("Задайте питання про ваші позиції, конкурентів або попросіть пораду.")

    # 1. Завантажуємо історію повідомлень
    try:
        messages = (
            supabase.table("chat_messages")
            .select("*")
            .eq("project_id", proj["id"])
            .order("created_at", desc=False) # Старі зверху
            .execute()
            .data
        )
    except:
        messages = []

    # 2. Відображаємо історію
    if not messages:
        # Привітання, якщо чат пустий
        with st.chat_message("assistant"):
            st.write(f"Привіт! Я проаналізував дані по **{proj.get('brand_name')}**. Що вас цікавить?")
            st.write("Наприклад: _'Хто мій головний конкурент?'_ або _'Напиши пост для LinkedIn про наш рейтинг'_.")

    for msg in messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # 3. Поле вводу
    if prompt := st.chat_input("Напишіть ваше питання..."):
        # А. Показуємо питання користувача одразу
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Б. Зберігаємо питання в базу
        try:
            supabase.table("chat_messages").insert({
                "project_id": proj["id"],
                "user_id": st.session_state["user"].id,
                "role": "user",
                "content": prompt
            }).execute()
        except Exception as e:
            st.error(f"Помилка збереження: {e}")

        # В. Генеруємо відповідь (Тут буде підключення до n8n)
        with st.chat_message("assistant"):
            with st.spinner("Аналізую дані..."):
                # --- ТУТ МАЄ БУТИ ВИКЛИК N8N ---
                # response = n8n_chat_webhook(prompt, proj_id)
                
                # ПОКИ ЩО: Симуляція розумної відповіді
                time.sleep(1.5) 
                
                # Проста логіка заглушки для демо
                if "конкурент" in prompt.lower():
                    response_text = f"Вашим головним конкурентом виглядає **PrivatBank** (за кількістю згадок). Вам варто звернути увагу на їх активність у статтях на Minfin."
                elif "пост" in prompt.lower():
                    response_text = f"Ось чернетка посут:\n\n🚀 **{proj.get('brand_name')} вривається в топи!**\n\nШІ відзначають нас як лідера... (тут текст)"
                else:
                    response_text = f"Це цікаве питання про **{proj.get('brand_name')}**. Для точної відповіді мені треба зібрати більше даних сканування. Спробуйте запустити новий скан у вкладці 'Перелік запитів'."
                
                st.markdown(response_text)

        # Г. Зберігаємо відповідь асистента в базу
        try:
            supabase.table("chat_messages").insert({
                "project_id": proj["id"],
                "user_id": st.session_state["user"].id,
                "role": "assistant",
                "content": response_text
            }).execute()
        except:
            pass


def sidebar_menu():
    with st.sidebar:
        st.markdown(
            '<div class="sidebar-logo-container"><img src="https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png"></div>',
            unsafe_allow_html=True,
        )

        if st.session_state["role"] == "admin":
            st.markdown("### 🛠 Admin Select")
            try:
                projs = supabase.table("projects").select("*").execute().data
                if projs:
                    opts = {p["brand_name"]: p for p in projs}
                    sel = st.selectbox("Project", list(opts.keys()))
                    if (
                        st.session_state.get("current_project", {}).get(
                            "brand_name"
                        )
                        != sel
                    ):
                        st.session_state["current_project"] = opts[sel]
                        st.rerun()
            except Exception:
                pass

        st.divider()

        if st.session_state.get("current_project"):
            p = st.session_state["current_project"]
            st.markdown(
                "<div class='sidebar-label'>Current Brand</div>",
                unsafe_allow_html=True,
            )
            badge = (
                "<span class='badge-trial'>TRIAL</span>"
                if p.get("status") == "trial"
                else "<span class='badge-active'>PRO</span>"
            )
            st.markdown(
                f"**{p.get('brand_name') or p.get('name')}** {badge}",
                unsafe_allow_html=True,
            )

            if p.get("status") == "trial":
                st.markdown(
                    '<a href="mailto:hi@virshi.ai" class="upgrade-btn">⭐ Підвищити план</a>',
                    unsafe_allow_html=True,
                )
            st.divider()

        opts = [
            "Дашборд",
            "Перелік запитів",
            "Джерела",
            "Конкуренти",
            "Рекомендації",
        ]
        icons = ["speedometer2", "list-ul", "hdd-network", "people", "lightbulb"]

        opts.append("GPT-Visibility")
        icons.append("robot")

        if st.session_state["role"] == "admin":
            opts.append("Адмін")
            icons.append("shield-lock")

        default_index = 0
        if st.session_state.get("force_page") in opts:
            default_index = opts.index(st.session_state["force_page"])
            st.session_state["force_page"] = None

        selected = option_menu(
            menu_title=None,
            options=opts,
            icons=icons,
            menu_icon="cast",
            default_index=default_index,
            styles={
                "nav-link-selected": {"background-color": "#8041F6"},
                "container": {"padding": "0!important"},
            },
        )
        st.divider()

        if st.session_state["user"]:
            d = st.session_state.get("user_details", {})
            full = f"{d.get('first_name','')} {d.get('last_name','')}".strip()
            st.markdown(
                f"<div class='sidebar-name'>{full}</div>", unsafe_allow_html=True
            )
            st.markdown("**Support:** [hi@virshi.ai](mailto:hi@virshi.ai)")
            if st.button("Вийти"):
                logout()

    return selected


# =========================
# 10. ROUTER
# =========================


def main():
    check_session()

    if not st.session_state["user"]:
        login_page()

    elif (
        st.session_state.get("current_project") is None
        and st.session_state["role"] != "admin"
    ):
        with st.sidebar:
            if st.button("Вийти"):
                logout()
        onboarding_wizard()

    else:
        if st.session_state["role"] == "admin" and not st.session_state.get(
            "current_project"
        ):
            pass

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
            st.title("🛡️ Admin Panel")
            try:
                df = pd.DataFrame(
                    supabase.table("projects").select("*").execute().data
                )
                st.dataframe(df, use_container_width=True)
            except Exception:
                st.error("Помилка доступу до БД.")


if __name__ == "__main__":
    main()
