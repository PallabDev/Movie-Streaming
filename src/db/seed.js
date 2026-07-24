import 'dotenv/config';
import { Pool } from 'pg';
import bcrypt from 'bcrypt';
import { drizzle } from 'drizzle-orm/node-postgres';
import * as schema from './schema.js';
import { users } from './schema.js';
import { eq } from 'drizzle-orm';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const db = drizzle(pool, { schema });

async function seed() {
  console.log('Seeding database...');

  // Create sessions table for connect-pg-simple
  await pool.query(`
    CREATE TABLE IF NOT EXISTS "sessions" (
      "sid" varchar NOT NULL COLLATE "default",
      "sess" json NOT NULL,
      "expire" timestamp(6) NOT NULL,
      CONSTRAINT "sessions_pkey" PRIMARY KEY ("sid")
    );
    CREATE INDEX IF NOT EXISTS "IDX_session_expire" ON "sessions" ("expire");
  `);
  console.log('✅ Sessions table ready');

  // Seed admin user
  const email = 'watch@watch.in';
  const [existing] = await db.select().from(users).where(eq(users.email, email));

  if (!existing) {
    const hashed = await bcrypt.hash('HexWatch78', 10);
    await db.insert(users).values({
      name: 'Admin',
      email,
      password: hashed,
      role: 'admin',
      hasAccess: true,
      streamLimit: 999,
    });
    console.log(`✅ Seeded admin: ${email} / HexWatch78`);
  } else {
    console.log(`ℹ️  Admin ${email} already exists, skipping`);
  }

  await pool.end();
  console.log('Done!');
}

seed().catch(err => { console.error('Seed failed:', err); process.exit(1); });
