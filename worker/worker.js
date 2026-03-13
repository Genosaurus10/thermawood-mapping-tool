/**
 * Thermawood GHL API Proxy — Cloudflare Worker
 *
 * Proxies requests to Go High Level API v2 from the mapping tool.
 * The GHL API key is stored as an encrypted environment variable (GHL_API_KEY).
 * The GHL Location ID is stored as an environment variable (GHL_LOCATION_ID).
 *
 * Endpoints:
 *   GET  /api/contacts?page=1        — Paginated contacts (100 per page)
 *   GET  /api/contacts/all           — All contacts (auto-paginates)
 *   GET  /api/opportunities?page=1   — Paginated opportunities
 *   GET  /api/opportunities/all      — All opportunities (auto-paginates)
 *   GET  /api/pipelines              — All pipelines and stages
 *   GET  /api/custom-fields          — Custom field definitions
 *   GET  /api/health                 — Health check
 */

const GHL_BASE = 'https://services.leadconnectorhq.com';
const GHL_VERSION = '2021-07-28';

// CORS headers — allow your GitHub Pages domain
function corsHeaders(request, env) {
  const allowedOrigins = [
    'https://genosaurus10.github.io',
    'http://localhost:3000',
    'http://localhost:8080',
    'http://127.0.0.1:3000',
  ];
  const origin = request.headers.get('Origin') || '';
  const allowed = allowedOrigins.includes(origin) ? origin : allowedOrigins[0];

  return {
    'Access-Control-Allow-Origin': allowed,
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '86400',
  };
}

// GHL API request helper
async function ghlFetch(path, env, method = 'GET', body = null) {
  const headers = {
    'Authorization': `Bearer ${env.GHL_API_KEY}`,
    'Version': GHL_VERSION,
    'Content-Type': 'application/json',
    'Accept': 'application/json',
  };

  const opts = { method, headers };
  if (body) opts.body = JSON.stringify(body);

  const resp = await fetch(`${GHL_BASE}${path}`, opts);

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`GHL API ${resp.status}: ${text}`);
  }

  return resp.json();
}

// Fetch one page of contacts via POST /contacts/search
async function fetchContactsPage(env, page = 1) {
  const data = await ghlFetch('/contacts/search', env, 'POST', {
    locationId: env.GHL_LOCATION_ID,
    page: page,
    limit: 100,
  });
  return data;
}

// Fetch ALL contacts (auto-paginate)
async function fetchAllContacts(env) {
  const allContacts = [];
  let page = 1;
  let hasMore = true;

  while (hasMore) {
    const data = await fetchContactsPage(env, page);
    const contacts = data.contacts || [];
    allContacts.push(...contacts);

    // GHL returns fewer than limit when no more pages
    if (contacts.length < 100) {
      hasMore = false;
    } else {
      page++;
      // Safety: cap at 1000 pages (100K contacts)
      if (page > 1000) break;
    }

    // Small delay to respect rate limits (100 req / 10 sec)
    if (hasMore) await new Promise(r => setTimeout(r, 120));
  }

  return allContacts;
}

// Fetch one page of opportunities
async function fetchOpportunitiesPage(env, page = 1, pipelineId = null) {
  let path = `/opportunities/search?location_id=${env.GHL_LOCATION_ID}&limit=100&page=${page}`;
  if (pipelineId) path += `&pipeline_id=${pipelineId}`;
  return ghlFetch(path, env);
}

// Fetch ALL opportunities (auto-paginate)
async function fetchAllOpportunities(env) {
  const allOpps = [];
  let page = 1;
  let hasMore = true;

  while (hasMore) {
    const data = await fetchOpportunitiesPage(env, page);
    const opps = data.opportunities || [];
    allOpps.push(...opps);

    if (opps.length < 100) {
      hasMore = false;
    } else {
      page++;
      if (page > 500) break;
    }

    if (hasMore) await new Promise(r => setTimeout(r, 120));
  }

  return allOpps;
}

// Fetch pipelines
async function fetchPipelines(env) {
  return ghlFetch(`/opportunities/pipelines?locationId=${env.GHL_LOCATION_ID}`, env);
}

// Fetch custom fields
async function fetchCustomFields(env) {
  return ghlFetch(`/locations/${env.GHL_LOCATION_ID}/customFields`, env);
}

// Main request handler
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    const cors = corsHeaders(request, env);

    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: cors });
    }

    // Only allow GET requests
    if (request.method !== 'GET') {
      return new Response(JSON.stringify({ error: 'Method not allowed' }), {
        status: 405,
        headers: { ...cors, 'Content-Type': 'application/json' },
      });
    }

    try {
      let result;

      switch (path) {
        case '/api/health':
          result = {
            status: 'ok',
            locationId: env.GHL_LOCATION_ID ? '***' + env.GHL_LOCATION_ID.slice(-4) : 'NOT SET',
            apiKeySet: !!env.GHL_API_KEY,
            timestamp: new Date().toISOString(),
          };
          break;

        case '/api/contacts': {
          const page = parseInt(url.searchParams.get('page') || '1');
          result = await fetchContactsPage(env, page);
          break;
        }

        case '/api/contacts/all':
          result = { contacts: await fetchAllContacts(env) };
          result.total = result.contacts.length;
          break;

        case '/api/opportunities': {
          const page = parseInt(url.searchParams.get('page') || '1');
          const pipelineId = url.searchParams.get('pipeline_id');
          result = await fetchOpportunitiesPage(env, page, pipelineId);
          break;
        }

        case '/api/opportunities/all':
          result = { opportunities: await fetchAllOpportunities(env) };
          result.total = result.opportunities.length;
          break;

        case '/api/pipelines':
          result = await fetchPipelines(env);
          break;

        case '/api/custom-fields':
          result = await fetchCustomFields(env);
          break;

        default:
          return new Response(JSON.stringify({ error: 'Not found', availableEndpoints: [
            '/api/health', '/api/contacts', '/api/contacts/all',
            '/api/opportunities', '/api/opportunities/all',
            '/api/pipelines', '/api/custom-fields'
          ]}), {
            status: 404,
            headers: { ...cors, 'Content-Type': 'application/json' },
          });
      }

      return new Response(JSON.stringify(result), {
        headers: { ...cors, 'Content-Type': 'application/json' },
      });

    } catch (err) {
      return new Response(JSON.stringify({ error: err.message }), {
        status: 502,
        headers: { ...cors, 'Content-Type': 'application/json' },
      });
    }
  },
};
