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

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Submit Ticket", "Tickets", "Product Catalog", "Generated Assets", "LLMOps"
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
    st.dataframe(catalog, use_container_width=True, hide_index=True)

    @st.dialog("Product Comparison", width="large")
    def show_comparison(product_id):
        conn = connect_to_db()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Original
        cur.execute("""
            SELECT id, title, description, price, category, image, rating, ingested_at
            FROM raw.products
            WHERE id = %s
        """, (product_id,))
        original = cur.fetchone()

        # Enriched
        cur.execute("""
            SELECT seo_description, brand_consistency_score, brand_score_reasoning,
                   item_subcategory, item_tags, target_audience, qa_flags, enriched_at
            FROM enriched.product_enrichments
            WHERE product_id = %s
        """, (product_id,))
        enriched = cur.fetchone()
        conn.close()

        if not original:
            st.error("Product not found.")
            return

        # Two columns side by side
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Original Listing")
            st.image(original["image"], width=200)
            st.markdown(f"**{original['title']}**")
            st.markdown(f"**Price:** ${original['price']:.2f}")
            st.markdown(f"**Category:** {original['category']}")
            st.markdown(f"**Description:**")
            st.text(original["description"])
            rating = json.loads(original["rating"]) if isinstance(original["rating"], str) else original["rating"]
            st.markdown(f"**Rating:** {rating.get('rate', 'N/A')}/5 ({rating.get('count', 0)} reviews)")

        with col2:
            st.subheader("Enriched Data")
            if enriched:
                st.markdown(f"**SEO Description:**")
                st.text(enriched["seo_description"])
                st.markdown(f"**Brand Score:** {enriched['brand_consistency_score']}/100")
                st.caption(enriched["brand_score_reasoning"])
                st.markdown(f"**Subcategory:** {enriched['item_subcategory']}")
                st.markdown(f"**Target Audience:** {enriched['target_audience']}")

                tags = enriched["item_tags"] if isinstance(enriched["item_tags"], list) else json.loads(enriched["item_tags"])
                st.markdown(f"**Tags:** {', '.join(tags)}")

                flags = enriched["qa_flags"] if isinstance(enriched["qa_flags"], list) else json.loads(enriched["qa_flags"])
                if flags:
                    st.warning(f"**QA Flags:** {', '.join(flags)}")
                else:
                    st.success("No QA issues found")

                st.caption(f"Enriched at: {enriched['enriched_at']}")
            else:
                st.info("Not yet enriched.")

    # Product selector + button
    selected_product = st.selectbox(
        "Select a product to compare",
        options=catalog["product_id"].tolist(),
        format_func=lambda x: catalog[catalog["product_id"] == x]["title"].values[0],
        key="product_compare",
    )

    if st.button("View Original vs Enriched"):
        show_comparison(selected_product)
    
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
 
# LLMOps
with tab5:
    st.subheader("LLMOps")
    model_stats = query_df("""
        SELECT model,
            avg_latency_sec,
            avg_total_tokens_per_sec,
            success_rate,
            success_count,
            failure_count
        FROM analytics.model_stats;
    """)
    st.dataframe(model_stats, use_container_width=True)
    
    st.subheader("Enrichment Errors")
    enrichment_errors = query_df("""
        SELECT id, product_id, model, enriched_at, error_message
        FROM enriched.llm_metrics
        WHERE is_success = false
        ORDER BY enriched_at DESC                        
    """)
    st.dataframe(enrichment_errors, use_container_width=True)