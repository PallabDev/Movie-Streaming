import { Router } from 'express';
import { requireAuth } from '../middleware/auth.js';
import { db } from '../db/index.js';
import { streams, users } from '../db/schema.js';
import { eq, and, sql } from 'drizzle-orm';

const router = Router();

router.get('/:id', requireAuth, async (req, res) => {
  const [stream] = await db
    .select()
    .from(streams)
    .where(and(eq(streams.id, req.params.id), eq(streams.userId, req.session.user.id)));

  if (!stream) return res.redirect('/');

  res.render('stream', {
    title: stream.title,
    stream,
    user: req.session.user,
  });
});

router.post('/:id/start', requireAuth, async (req, res) => {
  try {
    const [stream] = await db
      .select()
      .from(streams)
      .where(and(eq(streams.id, req.params.id), eq(streams.userId, req.session.user.id)));

    if (!stream) return res.status(404).json({ success: false });
    if (stream.status === 'active') return res.json({ success: true });

    const anyActive = await db
      .select({ count: sql`count(*)::int` })
      .from(streams)
      .where(eq(streams.status, 'active'));

    if (anyActive[0].count > 0) {
      return res.status(400).json({ success: false, message: 'Another stream is already active' });
    }

    await db.update(streams)
      .set({ status: 'active', startedAt: new Date() })
      .where(eq(streams.id, req.params.id));

    res.json({ success: true });
  } catch (err) {
    console.error('Start stream error:', err);
    res.status(500).json({ success: false });
  }
});

router.post('/:id/end', requireAuth, async (req, res) => {
  try {
    const [stream] = await db
      .select()
      .from(streams)
      .where(and(eq(streams.id, req.params.id), eq(streams.userId, req.session.user.id)));

    if (!stream) return res.status(404).json({ success: false });

    const endedAt = new Date();
    const duration = stream.startedAt
      ? Math.floor((endedAt - new Date(stream.startedAt)) / 1000)
      : 0;

    await db.update(streams)
      .set({ status: 'ended', endedAt, duration })
      .where(eq(streams.id, req.params.id));

    if (duration >= 5400) {
      await db
        .update(users)
        .set({ streamsUsed: sql`${users.streamsUsed} + 1` })
        .where(eq(users.id, req.session.user.id));
    }

    req.session.user.streamsUsed = (req.session.user.streamsUsed || 0) + (duration >= 5400 ? 1 : 0);

    res.json({ success: true, duration });
  } catch (err) {
    console.error('End stream error:', err);
    res.status(500).json({ success: false });
  }
});

export default router;
