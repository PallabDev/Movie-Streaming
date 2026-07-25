import { pgTable, serial, text, boolean, integer, timestamp } from 'drizzle-orm/pg-core';

export const users = pgTable('users', {
    id: serial('id').primaryKey(),
    name: text('name').notNull(),
    email: text('email').notNull().unique(),
    password: text('password').notNull(),
    role: text('role').default('user').notNull(),
    hasAccess: boolean('has_access').default(false).notNull(),
    streamLimit: integer('stream_limit').default(5).notNull(),
    streamCount: integer('stream_count').default(0).notNull(),
    createdAt: timestamp('created_at').defaultNow().notNull()
});

export const streamSessions = pgTable('stream_sessions', {
    id: serial('id').primaryKey(),
    streamKey: text('stream_key').notNull(),
    hostId: integer('host_id').references(() => users.id),
    title: text('title').notNull(),
    isLive: boolean('is_live').default(true).notNull(),
    startedAt: timestamp('started_at').defaultNow().notNull(),
    endedAt: timestamp('ended_at'),
    durationSeconds: integer('duration_seconds').default(0),
    viewerCount: integer('viewer_count').default(0).notNull(),
    peakViewers: integer('peak_viewers').default(0).notNull(),
    totalChunks: integer('total_chunks').default(0).notNull(),
    chunks1080p: integer('chunks_1080p').default(0).notNull(),
    chunks720p: integer('chunks_720p').default(0).notNull(),
    chunks480p: integer('chunks_480p').default(0).notNull(),
    failureCount: integer('failure_count').default(0).notNull(),
    countedAgainstLimit: boolean('counted_against_limit').default(false).notNull()
});
