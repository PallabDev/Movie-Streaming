import bcrypt from 'bcryptjs';
import { db, pool } from './index.js';
import { users } from './schema.js';
import { eq } from 'drizzle-orm';

export async function initDb() {
    try {
        console.log('🗄️ Initializing PostgreSQL Database tables...');

        await pool.query(`
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                password TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                has_access BOOLEAN NOT NULL DEFAULT false,
                stream_limit INTEGER NOT NULL DEFAULT 5,
                stream_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS stream_sessions (
                id SERIAL PRIMARY KEY,
                stream_key TEXT NOT NULL,
                host_id INTEGER REFERENCES users(id),
                title TEXT NOT NULL,
                is_live BOOLEAN NOT NULL DEFAULT true,
                started_at TIMESTAMP NOT NULL DEFAULT NOW(),
                ended_at TIMESTAMP,
                duration_seconds INTEGER DEFAULT 0,
                viewer_count INTEGER NOT NULL DEFAULT 0,
                peak_viewers INTEGER NOT NULL DEFAULT 0,
                total_chunks INTEGER NOT NULL DEFAULT 0,
                failure_count INTEGER NOT NULL DEFAULT 0,
                counted_against_limit BOOLEAN NOT NULL DEFAULT false
            );

            ALTER TABLE stream_sessions ADD COLUMN IF NOT EXISTS peak_viewers INTEGER NOT NULL DEFAULT 0;
            ALTER TABLE stream_sessions ADD COLUMN IF NOT EXISTS total_chunks INTEGER NOT NULL DEFAULT 0;
            ALTER TABLE stream_sessions ADD COLUMN IF NOT EXISTS failure_count INTEGER NOT NULL DEFAULT 0;

            -- Drop unused columns (safe to fail if already dropped)
            ALTER TABLE stream_sessions DROP COLUMN IF EXISTS chunks_1080p;
            ALTER TABLE stream_sessions DROP COLUMN IF EXISTS chunks_720p;
            ALTER TABLE stream_sessions DROP COLUMN IF EXISTS chunks_480p;
        `);

        // Seed Admin user: watch@watch.in / HexWatch78
        const adminEmail = 'watch@watch.in';
        const existingAdmin = await db.select().from(users).where(eq(users.email, adminEmail));

        if (existingAdmin.length === 0) {
            const hashedPassword = await bcrypt.hash('HexWatch78', 10);
            await db.insert(users).values({
                name: 'System Admin',
                email: adminEmail,
                password: hashedPassword,
                role: 'admin',
                hasAccess: true,
                streamLimit: 99999,
                streamCount: 0
            });
            console.log('👑 Admin user seeded: watch@watch.in / HexWatch78');
        } else {
            console.log('👑 Admin user verified: watch@watch.in');
        }

        console.log('✅ PostgreSQL Database schema and seeding complete.');
    } catch (err) {
        console.error('❌ Database initialization error:', err);
    }
}
