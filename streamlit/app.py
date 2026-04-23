import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import os
import json

def connect_to_db():
    print('Connecting to PostgreSQL')
    try:
        return psycopg2.connect(
            host=os.environ['DB_HOST'],
            port=os.environ['DB_PORT'],
            dbname=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
        )
    except psycopg2.Error as e:
        print(f'Database connection failed: {e}')
        raise
    
def query_df(sql) -> pd.DataFrame:
    conn = connect_to_db()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df

def asset_subheader(task_type, product_title, ticket_id):
    if task_type == "create_email_campaign":
        task_type_formatted = "Email Campaign"
    elif task_type == "create_landing_page":
        task_type_formatted = "Landing Page"
    elif task_type == "generate_alt_text":
        task_type_formatted = "Alt Text"
    elif task_type == "generate_meta_tags":
        task_type_formatted = "Meta Tags"
    elif task_type == "brand_review":
        task_type_formatted = "Brand Review"
    return f"{task_type_formatted} for {product_title} (Ticket ID: {ticket_id})"

st.set_page_config(page_title="Content Intelligence Pipeline", layout="wide")
st.title("Content Intelligence Pipeline")

tab1, tab2, tab3, tab4 = st.tabs([
    "Submit Ticket", "Tickets", "Product Catalog", "Generated Assets"
])

# Submit Ticket
with tab1:
    st.subheader("Create a new ticket")
    
    products = query_df("""
        SELECT product_id, title
        FROM analytics.product_catalog
        ORDER BY title 
    """)
    
    product = st.selectbox(
        "Product",
        options=products["product_id"].tolist(),
        format_func=lambda x: products[products["product_id"] == x]["title"].values[0],
    )
    
    task = st.selectbox("Task", [
        "create_landing_page",
        "create_email_campaign",
        "generate_alt_text",
        "generate_meta_tags",
        "brand_review",
    ])
    
    priority = st.selectbox("Priority", ["high", "medium", "low"])
    
    if st.button("Submit Ticket"):
        conn = connect_to_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO raw.tickets (product_id, task, priority, status, completed)
            VALUES (%s, %s, %s, 'Pending', FALSE)            
        """, (product, task, priority))
        conn.commit()
        conn.close()
        st.success("Ticket submitted. An agent will be assigned shortly.")
        
# Tickets
with tab2:
    st.subheader("Ticket Queue")
    tickets = query_df("""
        SELECT t.id as ticket_id,
               t.product_id,
               pc.title as product_title,
               t.task,
               t.priority,
               t.status
        FROM raw.tickets t
        INNER JOIN analytics.product_catalog pc ON t.product_id = pc.product_id
        ORDER BY CASE WHEN priority = 'high' THEN 1
                      WHEN priority = 'medium' THEN 2
                      ELSE 3 END
    """)
    st.dataframe(tickets, use_container_width=True)
    
# Product Catalog
with tab3:
    st.subheader("Enriched Product Catalog")
    catalog = query_df("SELECT * FROM analytics.product_catalog LIMIT 50")
    st.dataframe(catalog, use_container_width=True)
    
# Generated Assets
with tab4:
    st.subheader("Generated Assets")
    assets = query_df("""
        SELECT  gc.ticket_id,
                gc.product_id,
                pc.title as product_title,
                gc.task_type,
                gc.content_format,
                gc.input_tokens,
                gc.output_tokens,
                gc.latency_ms,
                gc.generated_at
        FROM assets.generated_content gc
        INNER JOIN analytics.product_catalog pc ON gc.product_id = pc.product_id
        ORDER BY generated_at DESC
    """)
    st.dataframe(assets, use_container_width=True)

    if not assets.empty:
        selected = st.selectbox("Select ticket ID to view asset content", assets["ticket_id"].tolist())
        if selected:
            conn = connect_to_db()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT content,
                       content_format,
                       task_type,
                       pc.title as product_title,
                       gc.ticket_id
                FROM assets.generated_content gc
                INNER JOIN analytics.product_catalog pc ON gc.product_id = pc.product_id
                WHERE ticket_id = %s""",
                (selected,),
            )
            row = cur.fetchone()
            conn.close()

            st.subheader(asset_subheader(row["task_type"], row["product_title"], row["ticket_id"]))

            if row["content_format"] == "json" and row["task_type"] == "create_email_campaign":
                email = json.loads(row["content"])
                
                # Email metadata
                st.markdown("**Subject Line:**")
                st.info(email["subject_line"])
                
                st.markdown("**Preview Text:**")
                st.caption(email["preview_text"])
                
                # Rendered preview
                st.markdown("---")
                st.markdown("**Email Preview:**")
                st.components.v1.html(email["html_body"], height=800, scrolling=True)
                
                # Raw HTML in expandable section
                with st.expander("View HTML source"):
                    st.code(email["html_body"], language="html")

            elif row["content_format"] == "html":
                st.components.v1.html(row["content"], height=600, scrolling=True)

            elif row["content_format"] == "json":
                st.json(json.loads(row["content"]))

            else:
                st.code(row["content"])