import json
import os
import anthropic
from typing import Literal
from datetime import datetime, UTC
from pydantic import BaseModel, Field, computed_field
from psycopg2.extras import RealDictCursor
import random

from utils.db_connect import connect_to_db

class Ticket(BaseModel):
    id: int
    product_id: int
    task: str
    priority: str
    product_title: str
    product_category: str
    product_subcategory: str
    product_price: str
    product_image: str
    product_seo_description: str
    product_rating_score: float
    product_rating_count: int
    product_item_tags: list[str]
    product_target_audience: str

class EmailCampaignOutput(BaseModel):
    subject_line: str = Field(description="Email subject line, under 60 characters, optimized for open rate.")
    preview_text: str = Field(description="Inbox preview snippet, under 90 characters.")
    html_body: str = Field(description="Complete HTML email body using table-based layout and inline CSS.")

class BrandReviewOutput(BaseModel):
    tone_score: int = Field(ge=0, le=100)
    tone_reasoning: str
    completeness_score: int = Field(ge=0, le=100)
    completeness_reasoning: str
    seo_score: int = Field(ge=0, le=100)
    seo_reasoning: str
    accessibility_score: int = Field(ge=0, le=100)
    accessibility_reasoning: str
    cross_channel_score: int = Field(ge=0, le=100)
    cross_channel_reasoning: str
    overall_score: int = Field(ge=0, le=100, description="Weighted: tone 25%, completeness 25%, SEO 20%, accessibility 15%, cross-channel 15%.")
    issues: list[str] = Field(description="Specific, actionable fixes.")
    verdict: str = Field(description="One of: 'Approved for publishing', 'Needs revision', 'Requires rewrite'.")

class MetaTagsOutput(BaseModel):
    title_tag: str = Field(description="50-60 characters, primary keyword front-loaded.")
    meta_description: str = Field(description="140-155 characters with value proposition.")
    meta_keywords: list[str] = Field(description="Derived from item tags.")
    og_title: str
    og_description: str
    og_image: str
    twitter_title: str
    twitter_description: str
    twitter_image: str

class GeneratedContent(BaseModel):
    ticket_id: int
    product_id: int
    task_type: str
    content: str
    content_format: str
    model: str
    input_tokens: int
    output_tokens: int
    latency_ms: float
    generated_at: datetime

TASK_TOOLS = {
    "create_email_campaign": {
        "name": "email_campaign",
        "description": "Record the generated email campaign.",
        "input_schema": EmailCampaignOutput.model_json_schema(),
    },
    "brand_review": {
        "name": "brand_review",
        "description": "Record the brand review results.",
        "input_schema": BrandReviewOutput.model_json_schema(),
    },
    "generate_meta_tags": {
        "name": "meta_tags",
        "description": "Record the generated meta tags.",
        "input_schema": MetaTagsOutput.model_json_schema(),
    },
}

TASK_PARSERS = {
    "create_email_campaign": EmailCampaignOutput,
    "brand_review": BrandReviewOutput,
    "generate_meta_tags": MetaTagsOutput,
}

TASK_PROMPTS = {

    "create_landing_page": """You are a senior front-end marketing engineer who builds high-converting product landing pages for enterprise retail brands.

You will receive enriched product data including SEO-optimized descriptions, tags, target audience, and pricing. Generate a complete, self-contained HTML landing page that includes:

Structure:
- Hero section with product title, price, and a prominent CTA button
- Product description section using the SEO-optimized copy
- Key features/benefits section derived from the item tags
- Social proof section incorporating the rating score and review count
- A clear "Add to Cart" or "Buy Now" CTA

Requirements:
- Use inline CSS only — the output must be a single self-contained HTML file
- Use a clean, modern design with professional typography and whitespace
- Include a placeholder <img> tag referencing the provided product image URL
- Include proper semantic HTML (header, main, section, footer)
- Make the page mobile-responsive using CSS media queries
- Target the specified audience in tone and visual hierarchy — e.g. a page for "outdoor enthusiasts" should feel different from one targeting "professional women"
- Do not invent features, materials, or claims not present in the provided data
- Do not include JavaScript""",

    "create_email_campaign": """You are a senior email marketing strategist who writes high-performing promotional emails for enterprise retail brands.

You will receive enriched product data including SEO descriptions, pricing, tags, ratings, and target audience. Generate a complete HTML email promoting this product.

Structure:
- Compelling subject line (provided separately in your output)
- Preview text (the snippet shown in inbox before opening)
- Header with brand-appropriate greeting
- Hero section featuring the product with a clear value proposition
- 2-3 benefit bullets derived from item tags and description
- Social proof callout using rating score and review count
- Single clear CTA button linking to a placeholder URL
- Footer with unsubscribe placeholder

Requirements:
- Use table-based layout for email client compatibility
- Use inline CSS only — no <style> blocks or external stylesheets
- Keep total copy under 200 words — emails that convert are concise
- Write the subject line to drive opens (curiosity, urgency, or specificity — not clickbait)
- Match tone to the target audience segment
- Do not invent product claims beyond what the provided data supports

Output format: Return a JSON object with keys "subject_line", "preview_text", and "html_body".""",

    "generate_alt_text": """You are a digital accessibility specialist who writes alt text for product images on enterprise e-commerce websites.

You will receive product metadata including the title, category, subcategory, and description. Generate alt text for the product's primary image.

Requirements:
- Write 1-2 concise sentences (under 125 characters is ideal, never exceed 150)
- Describe what a user would see in the image: the product itself, its color, shape, and context
- Include the product type and key visual attributes
- Do not start with "Image of" or "Photo of" — screen readers already announce it as an image
- Do not include pricing, ratings, or promotional language — alt text describes the visual, not the listing
- Do not hallucinate visual details you cannot confirm from the product title and category — describe only what is reasonably implied (e.g. "a backpack" from the title "Fjallraven Foldsack Backpack" is safe, but inventing a specific color is not)

Output format: Return only the alt text string, nothing else.""",

    "generate_meta_tags": """You are a technical SEO specialist who writes meta tags for product pages on enterprise e-commerce websites.

You will receive enriched product data including SEO descriptions, category, subcategory, tags, pricing, and target audience. Generate a complete set of HTML meta tags for this product's page.

Generate these specific tags:
- <title> — 50-60 characters, include product name and primary keyword naturally
- meta description — 140-155 characters, include a value proposition and soft CTA
- meta keywords — derived from item_tags, comma-separated
- Open Graph tags (og:title, og:description, og:type, og:image)
- Twitter Card tags (twitter:card, twitter:title, twitter:description, twitter:image)
- Canonical URL placeholder

Requirements:
- Do not keyword-stuff — meta description should read naturally to a human
- Title tag should front-load the most important keyword
- OG and Twitter descriptions can differ slightly from meta description to optimize for social sharing context
- Use the provided product image URL for og:image and twitter:image
- Use "product" as og:type

Output format: Return the complete set of meta tags as valid HTML, ready to paste into a <head> section.""",

    "brand_review": """You are a brand compliance auditor for a large enterprise retail organization. Your job is to evaluate whether a product listing meets publishing standards before it goes live.

You will receive the full enriched product data including the SEO-optimized description, tags, target audience, category, and pricing. Conduct a detailed brand review.

Evaluate against these criteria and score each 0-100:

1. Tone consistency — Does the copy match a professional, confident e-commerce voice? Is it appropriate for the target audience?
2. Content completeness — Does the listing include sufficient detail about the product's purpose, features, and differentiators?
3. SEO readiness — Are relevant keywords present naturally? Would this listing rank for its category?
4. Accessibility readiness — Could this listing support accessible publishing (clear language, logical structure, no jargon without context)?
5. Cross-channel readiness — Could this copy be adapted to email, social, and landing pages without a full rewrite?

Requirements:
- Provide a score and 1-2 sentence justification for each criterion
- Provide an overall weighted score (tone: 25%, completeness: 25%, SEO: 20%, accessibility: 15%, cross-channel: 15%)
- List specific, actionable fixes — not vague suggestions like "improve the copy"
- Flag any claims that seem fabricated or unsubstantiated by the source data
- End with a clear verdict: "Approved for publishing", "Needs revision", or "Requires rewrite"

Output format: Return a JSON object with keys "scores" (object with each criterion), "overall_score" (integer), "issues" (list of strings), "verdict" (string)."""
}


def system_prompt_for_task(task: str) -> str:
    if task not in TASK_PROMPTS:
        raise ValueError(f"Unknown task: {task}")
    return TASK_PROMPTS[task]

def build_user_message(ticket: Ticket) -> str:
    return f"""
    Product Title: {ticket.product_title}
    Product Category: {ticket.product_category}
    Product Subcategory: {ticket.product_subcategory}
    Product Price: {ticket.product_price}
    Product Image: {ticket.product_image}
    Product SEO Description: {ticket.product_seo_description}
    Product Rating Score: {ticket.product_rating_score}
    Product Rating Count: {ticket.product_rating_count}
    Product Item Tags: {ticket.product_item_tags}
    Product Target Audience: {ticket.product_target_audience}
    """
    
def table_exists(schema_name: str, table_name: str) -> bool:
    conn = connect_to_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(f"SELECT to_regclass('{schema_name}.{table_name}') IS NOT NULL AS check;")
    return cur.fetchone()['check']

def generate_content_for_ticket(
    ticket: Ticket,
    model: str = "claude-sonnet-4-6",
    max_tokens: int = 2048,
    temperature: float = 0.5
) -> GeneratedContent:
    # Initialize Anthropic API client
    client = anthropic.Anthropic()
    task = ticket.task

    # Build request
    request = {
        "model": model,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "system": system_prompt_for_task(task),
        "messages": [{"role": "user", "content": build_user_message(ticket)}],
    }

    # Add tool if applicable
    if task in TASK_TOOLS:
        tool_config = TASK_TOOLS[task]
        request["tools"] = [tool_config]
        request["tool_choice"] = {"type": "tool", "name": tool_config["name"]}

    # Start the clock
    start = datetime.now()
    message = client.messages.create(**request)
    latency_ms = (datetime.now() - start).total_seconds() * 1000
    input_tokens = message.usage.input_tokens
    output_tokens = message.usage.output_tokens

    # Parse the response
    if task in TASK_PARSERS:
        tool_block = next(b for b in message.content if b.type == "tool_use")
        content = json.dumps(tool_block.input)
        content_format = "json"
    else:
        content = message.content[0].text
        content_format = "html" if task == "create_landing_page" else "plaintext"
    
    content = content.strip()
    if content.startswith("```html"):
        content = content[7:]
    if content.endswith("```"):
        content = content[:-3]

    return GeneratedContent(
        ticket_id=ticket.id,
        product_id=ticket.product_id,
        task_type=task,
        content=content,
        content_format=content_format,
        model=model,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        latency_ms=latency_ms,
        generated_at=datetime.now(UTC)
    )

def generate_assets_for_tickets():
    try:
        # Connect to the database
        conn = connect_to_db()

        # Create assets schema if not exists
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS assets;
            CREATE TABLE IF NOT EXISTS assets.generated_content (
                id SERIAL PRIMARY KEY,
                ticket_id INTEGER,
                product_id INTEGER,
                task_type TEXT,
                content TEXT,
                content_format TEXT,    -- 'html', 'plaintext', 'json'
                model TEXT,
                input_tokens INTEGER,
                output_tokens INTEGER,
                latency_ms NUMERIC,
                generated_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        print('Assets schema and tables created')
        
        if table_exists('assets', 'tickets') and table_exists('analytics', 'product_catalog'):

            write_cur = conn.cursor()
            print('Retrieving tickets...')
            with conn.cursor('fetch_tickets', cursor_factory=RealDictCursor) as read_cur:
                read_cur.execute("""
                    SELECT t.id as ticket_id,
                        t.product_id,
                        t.task,
                        t.priority,
                        pc.title as product_title,
                        pc.category as product_category,
                        pc.subcategory as product_subcategory,
                        pc.price as product_price,
                        pc.image as product_image,
                        pc.seo_description as product_seo_description,
                        pc.rating_score as product_rating_score,
                        pc.rating_count as product_rating_count,
                        pc.item_tags as product_item_tags,
                        pc.target_audience as product_target_audience
                    FROM assets.tickets t
                    INNER JOIN analytics.product_catalog pc ON t.product_id = pc.product_id
                    WHERE t.completed = FALSE
                    AND EXISTS (
                        SELECT 1
                        FROM enriched.product_enrichments e
                        WHERE e.product_id = pc.product_id
                    )
                    ORDER BY CASE WHEN priority = 'high' THEN 1
                                WHEN priority = 'medium' THEN 2
                                ELSE 3 END,
                        id
                """)

                for row in read_cur:
                    ticket = Ticket(
                        id=row['ticket_id'],
                        product_id=row['product_id'],
                        task=row['task'],
                        priority=row['priority'],
                        product_title=row['product_title'],
                        product_category=row['product_category'],
                        product_subcategory=row['product_subcategory'],
                        product_price=row['product_price'],
                        product_image=row['product_image'],
                        product_seo_description=row['product_seo_description'],
                        product_rating_score=row['product_rating_score'],
                        product_rating_count=row['product_rating_count'],
                        product_item_tags=row['product_item_tags'],
                        product_target_audience=row['product_target_audience'],
                    )

                    write_cur.execute("""
                    UPDATE assets.tickets
                    SET status = 'In Progress'
                    WHERE id = %s
                    """, (ticket.id,))

                    model = "claude-sonnet-4-6"
                    max_tokens = 16384
                    temperature = 0.5
                    try:
                        generated_content = generate_content_for_ticket(ticket, model, max_tokens, temperature)
                    except Exception as e:
                        # Record failure to generate content
                        print(f"Failed to generate content for ticket {ticket.id}: {e}")
                        continue

                    # Record the generated content
                    write_cur.execute("""
                    INSERT INTO assets.generated_content
                        (ticket_id, product_id, task_type, content, content_format,
                        model, input_tokens, output_tokens, latency_ms, generated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    """, (
                        generated_content.ticket_id,
                        generated_content.product_id,
                        generated_content.task_type,
                        generated_content.content,
                        generated_content.content_format,
                        generated_content.model,
                        generated_content.input_tokens,
                        generated_content.output_tokens,
                        generated_content.latency_ms,
                    ))

                    # Update ticket status
                    write_cur.execute("""
                    UPDATE assets.tickets
                    SET completed = TRUE, status = 'Done'
                    WHERE id = %s
                    """, (generated_content.ticket_id,))

        conn.commit()
        print('Generated content inserted successfully')
    except Exception as e:
        print(f"Failed to generate content: {e}")
        raise
    finally:
        if 'write_cur' in locals():
            write_cur.close()
        if 'conn' in locals():
            conn.close()
            print('DB connection closed')