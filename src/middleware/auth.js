export function requireAuth(req, res, next) {
  if (!req.session?.user) {
    return res.redirect('/auth/login');
  }
  next();
}

export function requireAdmin(req, res, next) {
  if (!req.session?.user || req.session.user.role !== 'admin') {
    return res.status(403).render('partials/403', {
      title: 'Access Denied',
      user: req.session?.user || null,
    });
  }
  next();
}

export function injectUser(req, res, next) {
  res.locals.user = req.session?.user || null;
  next();
}
