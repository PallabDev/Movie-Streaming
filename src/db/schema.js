import { pgTable, varchar, boolean, integer, timestamp, uuid, pgEnum } from 'drizzle-orm/pg-core';

export const roleEnum = pgEnum('role', ['user', 'admin']);
export const streamStatusEnum = pgEnum('stream_status', ['idle', 'active', 'ended']);

export const users = pgTable('users', {
  id: uuid('id').primaryKey().defaultRandom(),
  name: varchar('name', { length: 255 }).notNull(),
  email: varchar('email', { length: 255 }).notNull().unique(),
  password: varchar('password', { length: 255 }).notNull(),
  role: roleEnum('role').default('user').notNull(),
  hasAccess: boolean('has_access').default(false).notNull(),
  streamsUsed: integer('streams_used').default(0).notNull(),
  streamLimit: integer('stream_limit').default(10).notNull(),
  lastResetAt: timestamp('last_reset_at').defaultNow().notNull(),
  createdAt: timestamp('created_at').defaultNow().notNull(),
});

export const streams = pgTable('streams', {
  id: uuid('id').primaryKey().defaultRandom(),
  streamKey: varchar('stream_key', { length: 255 }).notNull().unique(),
  title: varchar('title', { length: 255 }).notNull(),
  userId: uuid('user_id').notNull().references(() => users.id, { onDelete: 'cascade' }),
  status: streamStatusEnum('status').default('idle').notNull(),
  startedAt: timestamp('started_at'),
  endedAt: timestamp('ended_at'),
  duration: integer('duration').default(0),
  createdAt: timestamp('created_at').defaultNow().notNull(),
});
