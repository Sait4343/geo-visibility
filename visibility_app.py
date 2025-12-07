import time
from datetime import datetime, timedelta

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

# 🔴 ПРОДАКШН N8N ВЕБХУКИ (якщо хочеш — винеси в secrets)
N8N_GEN_URL = "https://virshi.app.n8n.cloud/webhook/webhook/generate-prompts"
N8N_ANALYZE_URL = "https://virshi.app.n8n.cloud/webhook-test/webhook/run-analysis"

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


def n8n_trigger_analysis(project_id, keywords, brand_name):
    """
    Відправляє 5 вибраних запитів на n8n для глибокого аналізу.
    n8n сам пише результати в Supabase.
    """
    try:
        user_email = st.session_state["user"].email
        payload = {
            "project_id": project_id,
            "keywords": keywords,
            "brand_name": brand_name,
            "user_email": user_email,
        }
        requests.post(N8N_ANALYZE_URL, json=payload, timeout=2)
        return True
    except requests.exceptions.ReadTimeout:
        return True
    except Exception as e:
        st.error(f"Помилка запуску аналізу: {e}")
        return False


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
                if res.user:
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
    except Exception as e:
        st.error(
            "Помилка входу: невірний логін, пароль або налаштування підтвердження email."
        )


def register_user(email: str, password: str, first: str, last: str) -> bool:
    """
    Реєстрація нового користувача + запис first_name / last_name в таблицю profiles.
    Без окремого екрану 'як до вас звертатись' — всі дані беремо з форми реєстрації.
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

            # Якщо email-confirmation вимкнено і є session
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
# 5. ONBOARDING (оновлений STEP 2 – бренд та домен)
# =========================


def onboarding_wizard():
    st.markdown("## 🚀 Налаштування Проекту")

    with st.container(border=True):
        step = st.session_state.get("onboarding_step", 2)

        # STEP 2 – дані про бренд (без запиту імені)
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

                            # 1. Створюємо проект (додаємо галузь і продукти)
                            res = (
                                supabase.table("projects")
                                .insert(
                                    {
                                        "user_id": user_id,
                                        "brand_name": st.session_state["temp_brand"],
                                        "domain": st.session_state["temp_domain"],
                                        "industry": st.session_state[
                                            "temp_industry"
                                        ],  # НОВЕ
                                        "products": st.session_state[
                                            "temp_products"
                                        ],  # НОВЕ
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
                                    {"project_id": proj_id, "keyword_text": kw}
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

    c1, c2 = st.columns([3, 1])
    with c1:
        st.title(f"Дашборд: {proj.get('brand_name', 'Brand')}")
    with c2:
        st.selectbox("Період:", ["Останні 7 днів", "Останні 30 днів"], index=0)
    st.markdown("---")

    sov, off, pos, pres, dom = 0, 0, 0, 0, 0
    try:
        stats = (
            supabase.table("dashboard_stats")
            .select("*")
            .eq("project_id", proj["id"])
            .execute()
            .data
        )
        if stats:
            s = stats[0]
            sov = s.get("sov", 0)
            off = s.get("official_source_pct", 0)
            pos = s.get("avg_position", 0)
            pres = s.get("brand_presence_pct", 0)
            dom = s.get("domain_mentions_pct", 0)
    except Exception:
        pass

    k1, k2, k3 = st.columns(3)
    with k1:
        with st.container(border=True):
            st.markdown("**ЧАСТКА ГОЛОСУ (SOV)**", help=METRIC_TOOLTIPS["sov"])
            c, ch = st.columns([1, 1])
            c.markdown(f"## {sov}%")
            ch.plotly_chart(
                get_donut_chart(sov), use_container_width=True, key="kpi_sov"
            )
    with k2:
        with st.container(border=True):
            st.markdown(
                "**% ОФІЦІЙНИХ ДЖЕРЕЛ**", help=METRIC_TOOLTIPS["official"]
            )
            c, ch = st.columns([1, 1])
            c.markdown(f"## {off}%")
            ch.plotly_chart(
                get_donut_chart(off), use_container_width=True, key="kpi_off"
            )
    with k3:
        with st.container(border=True):
            st.markdown(
                "**ЗАГАЛЬНИЙ НАСТРІЙ**", help=METRIC_TOOLTIPS["sentiment"]
            )
            fig = go.Figure(
                data=[
                    go.Pie(
                        labels=["Pos", "Neu", "Neg"],
                        values=[60, 30, 10],
                        hole=0,
                        marker_colors=["#00C896", "#9EA0A5", "#FF4B4B"],
                    )
                ]
            )
            fig.update_layout(
                height=80,
                margin=dict(t=0, b=0, l=0, r=0),
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True, key="kpi_sent")

    k4, k5, k6 = st.columns(3)
    with k4:
        with st.container(border=True):
            st.markdown(
                "**ПОЗИЦІЯ БРЕНДУ**", help=METRIC_TOOLTIPS["position"]
            )
            st.markdown(
                f"<h1 style='text-align: center; color: #8041F6;'>{pos}</h1>",
                unsafe_allow_html=True,
            )
            st.progress(int(100 - (pos * 10)) if pos else 0)
    with k5:
        with st.container(border=True):
            st.markdown(
                "**ПРИСУТНІСТЬ БРЕНДУ**", help=METRIC_TOOLTIPS["presence"]
            )
            c, ch = st.columns([1, 1])
            c.markdown(f"## {pres}%")
            ch.plotly_chart(
                get_donut_chart(pres), use_container_width=True, key="kpi_pres"
            )
    with k6:
        with st.container(border=True):
            st.markdown(
                "**ЗГАДКИ ДОМЕНУ**", help=METRIC_TOOLTIPS["domain"]
            )
            c, ch = st.columns([1, 1])
            c.markdown(f"## {dom}%")
            ch.plotly_chart(
                get_donut_chart(dom), use_container_width=True, key="kpi_dom"
            )

    st.markdown("### 📋 Моніторинг запитів")
    try:
        kws = (
            supabase.table("keywords")
            .select("keyword_text")
            .eq("project_id", proj["id"])
            .execute()
            .data
        )
        data = [{"Запит": k["keyword_text"], "Статус": "Active"} for k in kws]
    except Exception:
        data = []

    if not data:
        st.info("Дані ще збираються. Оновіть сторінку за хвилину.")
    else:
        st.dataframe(
            pd.DataFrame(data), use_container_width=True, hide_index=True
        )


# =========================
# 7. SIDEBAR
# =========================


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

        opts = ["Дашборд", "Перелік запитів", "Джерела", "Конкуренти", "Рекомендації"]
        icons = ["speedometer2", "list-ul", "hdd-network", "people", "lightbulb"]

        opts.append("GPT-Visibility")
        icons.append("robot")

        if st.session_state["role"] == "admin":
            opts.append("Адмін")
            icons.append("shield-lock")

        selected = option_menu(
            menu_title=None,
            options=opts,
            icons=icons,
            menu_icon="cast",
            default_index=0,
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
# 8. ROUTER
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
            st.title("📋 Перелік запитів")
            show_dashboard()
        elif page == "Джерела":
            st.title("📡 Джерела")
            st.info("У розробці...")
        elif page == "Конкуренти":
            st.title("⚔️ Конкуренти")
            st.info("У розробці...")
        elif page == "Рекомендації":
            st.title("💡 Рекомендації")
            st.info("У розробці...")
        elif page == "GPT-Visibility":
            st.title("🤖 GPT-Visibility")
            st.info("У розробці...")
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
