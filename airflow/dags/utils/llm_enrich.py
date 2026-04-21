import asyncio
import json
import anthropic
from typing import Literal
from datetime import datetime
from pydantic import BaseModel, Field, computed_field
from psycopg2.extras import RealDictCursor

from utils.insert_records import connect_to_db

class Product(BaseModel):
    id: int
    title: str
    description: str
    price: float
    category: str
    image: str
    rating: dict
    ingested_at: datetime
    
    @computed_field
    @property
    def rating_count(self) -> str:
        return self.rating.get('count')
    
    @computed_field
    @property
    def rating_score(self) -> str:
        return self.rating.get('rate')


class LLMOutputSchema(BaseModel):
    seo_description: str = Field(
        description="SEO-optimized product description, 2-3 sentences, including relevant keywords naturally."
    )
    brand_consistency_score: int = Field(
        ge=0,
        le=100,
        description="How well the original description matches professional e-commerce brand tone."
    )
    brand_score_reasoning: str = Field(
        description="Brief explanation of the brand consistency score."
    )
    item_subcategory: str = Field(
        description="Specific subcategory (e.g. 'hiking backpacks' not just 'backpacks')."
    )
    item_tags: list[str] = Field(
        description="5-8 short, relevant discovery tags for search and filtering."
    )
    target_audience: str = Field(
        description="Primary target customer segment for this product."
    )
    qa_flags: list[str] = Field(
        description="List of content quality issues found in the original listing."
    )
    
    
class LLMOpsMetrics(BaseModel):
    product_id: int
    model: str
    input_tokens: int
    output_tokens: int
    latency_ms: float
    enriched_at: datetime
    
    
class ProductEnrichment(BaseModel):
    enrichment: LLMOutputSchema
    metrics: LLMOpsMetrics
    
SYSTEM_PROMPT = """You are a senior e-commerce content strategist specializing in product catalog optimization for enterprise retail brands.

Your job is to evaluate and enrich product listings to meet enterprise content standards. You approach every listing as if it will be published on a major retailer's website where brand consistency, SEO performance, and content completeness directly impact revenue.

When scoring brand consistency, evaluate against these criteria:
- Professional, confident tone (not casual or generic)
- Specific, measurable claims (materials, dimensions, use cases)
- Active voice and benefit-driven language
- Consistent formatting and capitalization
- No filler phrases like "great product" or "you'll love it"

A score of 90-100 means publish-ready for a top-tier retailer. 
A score of 50-70 means usable but needs revision. 
Below 50 means a full rewrite is needed.

When generating QA flags, check for these specific issues:
- "too-short-description": fewer than 15 words
- "keyword-gaps": missing obvious search terms for the product type
- "vague-title": title doesn't specify what the product actually is
- "no-material-info": no mention of materials, fabric, or composition
- "missing-dimensions": no size, weight, or capacity information
- "missing-use-case": no mention of when/where/how to use the product
- "no-differentiator": nothing distinguishing this from competing products

Only flag issues that are actually present. An empty list is valid."""

def enrich_record(
    product: Product,
    model: str = "claude-sonnet-4-6",
    max_tokens: int = 2048,
    temperature: float = 0.5
):
    # Initialize Anthropic API client
    client = anthropic.Anthropic()
    
    # Start the clock
    start = datetime.now()
    
    # Call model to enrich product data
    message = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"""Enrich this product listing:

                                Product ID: {product.id}
                                Title: {product.title}
                                Category: {product.category}
                                Price: ${product.price:.2f}
                                Current Description: {product.description}
                                Customer Rating: {product.rating_score}/5 ({product.rating_count} reviews)

                                Provide your enrichment based on this listing exactly as it appears. Do not invent product details that aren't stated or reasonably implied by the title and description.""",
            }
        ],
        tools=[{
            "name": "record_enrichment",
            "description": "Record the enrichment results for this product listing.",
            "input_schema": LLMOutputSchema.model_json_schema()
        }],
        tool_choice={"type": "tool", "name": "record_enrichment"}
    )
    
    # Record LLM call latency for LLMOps reporting
    latency_ms = (datetime.now() - start).total_seconds() * 1000
    
    tool_use_block = next(b for b in message.content if b.type == "tool_use")
    enrichment = LLMOutputSchema(**tool_use_block.input)
    
    metrics = LLMOpsMetrics(
        product_id=product.id,
        model=model,
        input_tokens=message.usage.input_tokens,
        output_tokens=message.usage.output_tokens,
        latency_ms=latency_ms,
        enriched_at=datetime.now()
    )
    return ProductEnrichment(
        enrichment=enrichment,
        metrics=metrics
    )

def enrich_data():
    try:
        conn = connect_to_db()
        
        # Create enriched schema if not exists (enriched product descriptions and LLMOps metrics)
        cursor = conn.cursor()
        cursor.execute("""
            
            CREATE SCHEMA IF NOT EXISTS enriched;
            
            CREATE TABLE IF NOT EXISTS enriched.product_enrichments (
                product_id INTEGER PRIMARY KEY REFERENCES raw.products(id),
                seo_description TEXT,
                brand_consistency_score INTEGER,
                brand_score_reasoning TEXT,
                item_subcategory TEXT,
                item_tags JSONB,
                target_audience TEXT,
                qa_flags JSONB,
                enriched_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE TABLE IF NOT EXISTS enriched.llm_metrics (
                id SERIAL PRIMARY KEY,
                product_id INTEGER REFERENCES raw.products(id),
                model TEXT,
                input_tokens INTEGER,
                output_tokens INTEGER,
                latency_ms NUMERIC,
                enriched_at TIMESTAMP DEFAULT NOW()
            );
        """)

        # New cursor, retrieve records in small batches
        write_cur = conn.cursor()    
        with conn.cursor('fetch_batched_records', cursor_factory=RealDictCursor) as read_cur:
            read_cur.itersize = 10
            read_cur.execute("""
                SELECT id, title, description, price, category, image, rating, ingested_at
                FROM raw.products          
            """)
            for row in read_cur:
                product = Product(
                    id=row['id'],
                    title=row['title'],
                    description=row['description'],
                    price=row['price'],
                    category=row['category'],
                    image=row['image'],
                    rating=row['rating'],
                    ingested_at=row['ingested_at']
                )
                result = enrich_record(product)
                
                enrichment = result.enrichment
                metrics = result.metrics
                
                # Upsert enrichment
                write_cur.execute("""
                    INSERT INTO enriched.product_enrichments 
                        (product_id, seo_description, brand_consistency_score, 
                        brand_score_reasoning, item_subcategory, item_tags, 
                        target_audience, qa_flags)
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb)
                    ON CONFLICT (product_id) DO UPDATE SET
                        seo_description = EXCLUDED.seo_description,
                        brand_consistency_score = EXCLUDED.brand_consistency_score,
                        brand_score_reasoning = EXCLUDED.brand_score_reasoning,
                        item_subcategory = EXCLUDED.item_subcategory,
                        item_tags = EXCLUDED.item_tags,
                        target_audience = EXCLUDED.target_audience,
                        qa_flags = EXCLUDED.qa_flags,
                        enriched_at = NOW()
                """, (
                    product.id,
                    enrichment.seo_description,
                    enrichment.brand_consistency_score,
                    enrichment.brand_score_reasoning,
                    enrichment.item_subcategory,
                    json.dumps(enrichment.item_tags),
                    enrichment.target_audience,
                    json.dumps(enrichment.qa_flags)
                ))
                
                # Append only for LLMOps (record every run)
                write_cur.execute("""
                    INSERT INTO enriched.llm_metrics
                        (product_id, model, input_tokens, output_tokens, latency_ms)
                    VALUES (%s, %s, %s, %s, %s)                  
                """, (
                    metrics.product_id,
                    metrics.model,
                    metrics.input_tokens,
                    metrics.output_tokens,
                    metrics.latency_ms
                ))
                
        conn.commit()
    
    except Exception as e:
        print(f'Failed to enrich records: {e}')
        raise
    
    finally:
        if 'write_cur' in locals():
            write_cur.close()
        if 'conn' in locals():
            conn.close()
            print('DB connection closed')