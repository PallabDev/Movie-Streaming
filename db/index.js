import pg from 'pg';
import { drizzle } from 'drizzle-orm/node-postgres';
import * as schema from './schema.js';

const rawUrl = process.env.DATABASE_URL || 'postgresql://neondb_owner:npg_fQzZKsmU8P7W@ep-fragrant-frog-ayuj1vwd-pooler.c-5.us-east-2.aws.neon.tech/neondb?sslmode=verify-full';
const connectionString = rawUrl.replace(/sslmode=[^&]*/, 'sslmode=verify-full').replace(/&channel_binding=[^&]*/, '').replace(/\?channel_binding=[^&]*&?/, '?');

const pool = new pg.Pool({
    connectionString,
    ssl: { rejectUnauthorized: false }
});

export const db = drizzle(pool, { schema });
export { pool };
