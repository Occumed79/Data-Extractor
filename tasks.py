import json
from datetime import datetime

from app import crawl_site, db_conn, infer_site_type, insert_records, text, update_job


def run_scrape_job(job_id: str):
    with db_conn() as conn:
        row = conn.execute(text('SELECT * FROM jobs WHERE id = :job_id'), {'job_id': job_id}).mappings().fetchone()
    if not row:
        return {'status': 'missing', 'job_id': job_id}

    try:
        update_job(job_id, status='running', error_text='')
        urls = json.loads(row['urls_json'])
        detail_mode = bool(row['detail_mode'])
        max_pages = int(row['max_pages'])
        all_records = []
        for url in urls:
            site = row['site_type'] if row['site_type'] != 'auto' else infer_site_type(url)
            if site not in {'mdsave', 'zocdoc'}:
                continue
            all_records.extend(crawl_site(url, site, detail_mode, max_pages))
        insert_records(job_id, all_records)
        update_job(job_id, status='completed')
        return {
            'status': 'completed',
            'job_id': job_id,
            'record_count': len(all_records),
            'finished_at': datetime.utcnow().isoformat(),
        }
    except Exception as exc:
        update_job(job_id, status='failed', error_text=str(exc)[:4000])
        raise
