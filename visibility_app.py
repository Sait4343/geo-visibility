import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from supabase import create_client, Client
from streamlit_option_menu import option_menu
import time
import random
from datetime import datetime, timedelta

# --- 1. CONFIGURATION & STYLING ---
st.set_page_config(
    page_title="GEO-Analyst | AI Visibility Platform",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS to match the screenshots (Clean SaaS Look)
st.markdown("""
<style>
    /* Global Font & Background */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
        background-color: #f8f9fa; /* Light gray background */
        color: #1e293b;
    }

    /* Sidebar Styling */
    [data-testid="stSidebar"] {
        background-color: #ffffff;
        border-right: 1px solid #e2e8f0;
    }

    /* Metric Cards (Matching image_a984f3.png) */
    div.metric-card {
        background-color: white;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        text-align: center;
        transition: all 0.2s;
        height: 100%;
        border-left: 4px solid #10b981; /* Green accent */
    }
    div.metric-card:hover {
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .metric-label {
        color: #64748b;
        font-size: 0.85rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 10px;
    }
    .metric-value {
        color: #0f172a;
        font-size: 1.8rem;
        font-weight: 700;
    }
    
    /* Buttons (Purple/Blue from screenshots) */
    div.stButton > button {
        background-color: #6366f1;
        color: white;
        border-radius: 8px;
        border: none;
        padding: 0.5rem 1rem;
        font-weight: 600;
    }
    div.stButton > button:hover {
        background-color: #4f46e5;
        color: white;
    }

    /* Tables */
    [data-testid="stDataFrame"] {
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        background: white;
        padding: 10px;
    }

    /* Status Pills */
    .status-pill {
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 12px;
        font-weight: 600;
    }
    .status-completed { background-color: #d1fae5; color: #065f46; }
    .status-trial { background-color: #fef3c7; color: #92400e; }
    
    /* Custom Banner */
    .demo-banner {
        background: linear-gradient(90deg, #e0e7ff 0%, #fae8ff 100%);
        padding: 10px;
        border-radius: 8px;
        color: #4338ca;
        font-weight: 600;
        text-align: center;
        margin-bottom: 20px;
        border: 1px solid #c7d2fe;
    }
</style>
""", unsafe_allow_html=True)

# --- 2. SUPABASE CONNECTION & MOCK DATA ---
# Try to connect, if fails (no secrets), use Mock Mode for Demo
try:
    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    DB_CONNECTED = True
except:
    DB_CONNECTED = False
    # Mock Database for UI Demonstration
    st.session_state['mock_projects'] = [
        {'id': 1, 'brand_name': 'SkyUp', 'domain': 'skyup.aero', 'status': 'trial', 'created_at': '2025-09-25'}
    ]

# Session State Init
if 'user' not in st.session_state: st.session_state['user'] = None
if 'role' not in st.session_state: st.session_state['role'] = 'user'
if 'current_project' not in st.session_state: st.session_state['current_project'] = None
if 'page' not in st.session_state: st.session_state['page'] = 'Dashboard'

# --- 3. HELPER FUNCTIONS ---

def login_user(email, password):
    if DB_CONNECTED:
        try:
            res = supabase.auth.sign_in_with_password({"email": email, "password": password})
            st.session_state['user'] = res.user
            # Fetch Role
            profile = supabase.table('profiles').select('role').eq('id', res.user.id).single().execute()
            st.session_state['role'] = profile.data['role'] if profile.data else 'user'
            return True
        except Exception as e:
            st.error(f"Login Error: {e}")
            return False
    else:
        # Mock Login
        st.session_state['user'] = {'email': email, 'id': 'mock-id-123'}
        st.session_state['role'] = 'admin' if 'admin' in email else 'user'
        return True

def run_demo_scan(brand, domain):
    """Simulates the n8n Workflow A (5 Queries)"""
    with st.status("🚀 Launching AI Agents...", expanded=True) as status:
        st.write("🤖 Generating 5 strategic search queries via Gemini Flash...")
        time.sleep(1.5)
        st.write(f"🔍 Scanning Perplexity for '{brand}' mentions...")
        time.sleep(1.5)
        st.write("📊 Analyzing Sentiment & Source Authority...")
        time.sleep(1.0)
        st.write("💾 Saving results to Supabase...")
        time.sleep(0.5)
        status.update(label="✅ Analysis Complete!", state="complete", expanded=False)
    
    # Create mock project if in mock mode
    if not DB_CONNECTED:
        st.session_state['mock_projects'].append({
            'id': random.randint(100,999),
            'brand_name': brand,
            'domain': domain,
            'status': 'trial',
            'created_at': datetime.now().strftime("%Y-%m-%d")
        })
        st.session_state['current_project'] = st.session_state['mock_projects'][-1]

# --- 4. PAGE RENDERERS ---

def render_onboarding():
    st.markdown("<div style='text-align: center; margin-top: 50px;'>", unsafe_allow_html=True)
    st.title("Welcome to GEO-Analyst 🌍")
    st.subheader("See how AI sees your brand in 30 seconds")
    st.markdown("</div>", unsafe_allow_html=True)

    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.form("onboarding_form"):
            brand = st.text_input("Brand Name", placeholder="e.g. Nova Poshta")
            domain = st.text_input("Website URL", placeholder="e.g. novaposhta.ua")
            region = st.selectbox("Target Region", ["Ukraine", "USA", "Global"])
            submitted = st.form_submit_button("Start Free Demo Scan 🚀", use_container_width=True)
            
            if submitted and brand and domain:
                run_demo_scan(brand, domain)
                st.session_state['page'] = 'Dashboard'
                st.rerun()

def render_dashboard():
    proj = st.session_state['current_project']
    
    # Header
    c1, c2 = st.columns([3, 1])
    with c1:
        st.title(f"{proj['brand_name']}")
        st.caption(f"🌐 Entity: {proj['brand_name']} | 📅 Created: {proj['created_at']}")
    with c2:
        if proj.get('status') == 'trial':
            st.markdown(f"""
            <div style="background-color: #fffbeb; color: #b45309; padding: 10px; border-radius: 8px; text-align: center; border: 1px solid #fcd34d;">
                🔒 <b>Trial Mode</b><br>Limited to 5 queries
            </div>
            """, unsafe_allow_html=True)

    # 1. KPI GRID (Matches image_a984f3.png)
    st.markdown("### Performance Overview")
    
    # Mock Data for KPIs
    kpi_data = {
        "sov": 30.86, "official_src": 50.00, "sentiment": 78,
        "position": 1.7, "presence": 60.00, "domain_mentions": 10.00
    }

    # Custom HTML Card Function
    def metric_html(label, value, is_pie=False, is_text=False):
        content = f"<div class='metric-value'>{value}</div>"
        if is_pie:
            # Simple CSS Pie Chart simulation
            content = f"""
            <div style="display: flex; justify-content: center; align-items: center; height: 80px;">
                <div style="width: 60px; height: 60px; border-radius: 50%; background: conic-gradient(#10b981 {value}%, #e2e8f0 0);"></div>
            </div>
            <div style='font-size: 1.2rem; font-weight: bold;'>{value}%</div>
            """
        return f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            {content}
        </div>
        """

    r1c1, r1c2, r1c3 = st.columns(3)
    with r1c1: st.markdown(metric_html("Share of Voice (SOV)", f"{kpi_data['sov']}%", is_pie=True), unsafe_allow_html=True)
    with r1c2: st.markdown(metric_html("% Official Sources", f"{kpi_data['official_src']}%", is_pie=True), unsafe_allow_html=True)
    with r1c3: st.markdown(metric_html("Avg Sentiment", "Positive 😄"), unsafe_allow_html=True)

    st.write("") # Spacer

    r2c1, r2c2, r2c3 = st.columns(3)
    with r2c1: st.markdown(metric_html("Avg Brand Position", f"{kpi_data['position']}"), unsafe_allow_html=True)
    with r2c2: st.markdown(metric_html("Brand Presence", f"{kpi_data['presence']}%", is_pie=True), unsafe_allow_html=True)
    with r2c3: st.markdown(metric_html("Domain Mentions", f"{kpi_data['domain_mentions']}%", is_pie=True), unsafe_allow_html=True)

    st.markdown("---")

    # 2. CHARTS (Plotly)
    c_chart, c_score = st.columns([2, 1])
    
    with c_chart:
        st.subheader("Brand Position Evolution")
        # Mock Trend Data
        dates = pd.date_range(end=datetime.today(), periods=10).strftime("%b %d")
        df_trend = pd.DataFrame({
            "Date": dates,
            "Position": [4, 3.8, 3.5, 3.0, 2.8, 2.5, 2.0, 1.8, 1.7, 1.7]
        })
        fig = px.line(df_trend, x="Date", y="Position", markers=True)
        fig.update_layout(
            yaxis=dict(autorange="reversed"), # 1 is top
            plot_bgcolor="white",
            margin=dict(t=10, b=10, l=10, r=10),
            height=300
        )
        fig.update_traces(line_color='#6366f1', line_width=3)
        st.plotly_chart(fig, use_container_width=True)

    with c_score:
        st.subheader("LLMO Score")
        st.markdown("""
        <div style="background: white; border-radius: 12px; padding: 20px; text-align: center; border: 1px solid #e2e8f0;">
            <h1 style="font-size: 3rem; color: #6366f1; margin: 0;">82<span style="font-size: 1rem; color: #94a3b8;">/100</span></h1>
            <p style="color: #64748b;">Excellent Visibility</p>
            <hr>
            <div style="text-align: left; font-size: 0.9rem;">
                <div>🤖 <b>ChatGPT:</b> #1</div>
                <div>🌌 <b>Perplexity:</b> #2</div>
                <div>♊ <b>Gemini:</b> #1</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

def render_competitors():
    st.title("⚔️ Competitor Analysis")
    st.caption("See who you are fighting against in AI responses.")

    # Mock Data (Matches image_a98478.png)
    data = [
        {"Brand": "SkyUp", "Mentions": 50, "Sentiment": "Neutral", "Position": 1.0, "Trend": "⬆️"},
        {"Brand": "Ryanair", "Mentions": 20, "Sentiment": "Neutral", "Position": 7.2, "Trend": "⬇️"},
        {"Brand": "Wizz Air", "Mentions": 19, "Sentiment": "Neutral", "Position": 6.4, "Trend": "⬆️"},
        {"Brand": "Turkish Airlines", "Mentions": 6, "Sentiment": "Positive", "Position": 6.3, "Trend": "➖"},
    ]
    df = pd.DataFrame(data)

    col1, col2 = st.columns([3, 1])
    with col1:
        st.dataframe(
            df,
            column_config={
                "Position": st.column_config.NumberColumn("Avg Position", format="%.1f"),
                "Mentions": st.column_config.ProgressColumn("Volume", format="%d", min_value=0, max_value=60),
            },
            use_container_width=True,
            hide_index=True
        )
    with col2:
        st.info("💡 **Insight:** You are dominating SOV (50 mentions), but Ryanair is your closest threat in terms of pricing queries.")

def render_sources():
    st.title("📡 AI Source Intelligence")
    
    t1, t2 = st.tabs(["Owned Assets", "Earned Media"])
    
    with t1:
        st.subheader("Your Official Assets")
        # Matches image_a9845c.png
        df_owned = pd.DataFrame([
            {"URL": "skyup.aero", "Mentions": 18, "Status": "Verified ✅"},
            {"URL": "instagram.com/skyup", "Mentions": 11, "Status": "Verified ✅"},
        ])
        st.dataframe(df_owned, use_container_width=True)
    
    with t2:
        st.subheader("External References")
        # Matches AI Reference Sources.png
        df_earned = pd.DataFrame([
            {"Domain": "tripmydream.ua", "References": 20, "Type": "Ranking"},
            {"Domain": "dovkola.media", "References": 18, "Type": "Ranking"},
            {"Domain": "lowcostavia.com.ua", "References": 18, "Type": "Ranking"},
        ])
        st.dataframe(df_earned, use_container_width=True)

def render_recommendations():
    st.title("💡 Strategic Recommendations")
    
    # Kanban Style (Matches AI Recommendations.png)
    c1, c2, c3 = st.columns(3)
    
    with c1:
        st.markdown("### 🔴 To Do")
        st.info("**Website Collaboration**\n\nCreate a guest post for `tripmydream.ua` to improve backlink authority in Gemini.")
        st.warning("**Content Gap**\n\nAdd FAQ about 'Baggage Allowance' to match Ryanair's visibility.")
    
    with c2:
        st.markdown("### 🟡 In Progress")
        st.markdown("""
        <div style="background: white; padding: 10px; border-radius: 8px; border-left: 4px solid #facc15; box-shadow: 0 1px 2px rgba(0,0,0,0.1);">
            <b>Social Media Opps</b><br>
            <span style="font-size: 12px; color: gray;">PR Strategy</span><br>
            Generate buzz on TikTok regarding winter sales.
        </div>
        """, unsafe_allow_html=True)
        
    with c3:
        st.markdown("### 🟢 Done")
        st.success("**Technical SEO**\n\nSchema markup added for Flight schedules.")

def render_admin():
    st.title("🛡️ Super Admin Panel")
    st.markdown("Manage users, activations, and token usage.")
    
    # Mock Admin Data
    users = [
        {"Email": "client@skyup.aero", "Project": "SkyUp", "Status": "Trial", "Tokens Used": 4500},
        {"Email": "ceo@novaposhta.ua", "Project": "Nova Poshta", "Status": "Active", "Tokens Used": 125000},
    ]
    df = pd.DataFrame(users)
    
    edited_df = st.data_editor(
        df,
        column_config={
            "Status": st.column_config.SelectboxColumn("Status", options=["Trial", "Active", "Suspended"]),
            "Tokens Used": st.column_config.ProgressColumn("Cost", min_value=0, max_value=200000, format="%d"),
        },
        use_container_width=True,
        num_rows="dynamic"
    )
    
    if st.button("Save Changes"):
        st.toast("User statuses updated successfully!", icon="💾")

# --- 5. MAIN APP LOGIC ---

def main():
    # Sidebar Navigation
    with st.sidebar:
        st.image("https://img.icons8.com/?size=100&id=zN242j42f3rW&format=png", width=50) # Placeholder Logo
        st.markdown("### GEO-Analyst")
        
        # User Profile Mini
        if st.session_state['user']:
            st.markdown(f"👤 **{st.session_state['user']['email']}**")
            st.caption(f"Role: {st.session_state['role'].upper()}")
            
            # Demo Banner (Matches screenshot)
            if st.session_state.get('current_project', {}).get('status') == 'trial':
                st.markdown('<div class="demo-banner">✨ 14 Demo Days Left</div>', unsafe_allow_html=True)
                if st.button("💎 Upgrade to Pro", use_container_width=True):
                    st.toast("Contacting Sales Manager...", icon="📞")

        # Navigation Menu
        menu_options = ["Dashboard", "Competitors", "Sources", "Recommendations"]
        menu_icons = ["speedometer2", "people", "hdd-network", "lightbulb"]
        
        if st.session_state['role'] == 'admin':
            menu_options.append("Admin Panel")
            menu_icons.append("shield-lock")

        selected = option_menu(
            "Menu",
            menu_options,
            icons=menu_icons,
            menu_icon="cast",
            default_index=0,
            styles={
                "nav-link-selected": {"background-color": "#6366f1"},
            }
        )
        
        st.divider()
        if st.session_state['user']:
            if st.button("Log Out"):
                st.session_state['user'] = None
                st.rerun()

    # Routing Logic
    if not st.session_state['user']:
        # Login Screen
        c1, c2, c3 = st.columns([1,1,1])
        with c2:
            st.markdown("<br><br>", unsafe_allow_html=True)
            st.title("Sign In")
            email = st.text_input("Email")
            password = st.text_input("Password", type="password")
            if st.button("Login", use_container_width=True):
                if login_user(email, password):
                    st.rerun()
            st.markdown("---")
            st.caption("Don't have an account? Just login to start demo.")
            
    elif not st.session_state['current_project'] and st.session_state['role'] != 'admin':
        # Onboarding
        render_onboarding()
        
    else:
        # Main App
        if selected == "Dashboard":
            render_dashboard()
        elif selected == "Competitors":
            render_competitors()
        elif selected == "Sources":
            render_sources()
        elif selected == "Recommendations":
            render_recommendations()
        elif selected == "Admin Panel":
            render_admin()

if __name__ == "__main__":
    main()
