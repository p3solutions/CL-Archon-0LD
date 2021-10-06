package com.p3.archon.dboperations.dao;

import org.hibernate.Session;
import org.hibernate.Transaction;

import com.p3.archon.core.ArchonDBUtils;
import com.p3.archon.dboperations.dbmodel.MetalyzerSessions;

public class MetalyzerSessionsDAO {

	public boolean addRecord(MetalyzerSessions ms) {
		Transaction trns = null;
		boolean status = false;
		Session session = ArchonDBUtils.getSessionFactory().openSession();
		try {
			trns = session.beginTransaction();
			session.save(ms);
			session.getTransaction().commit();
			status = true;
		} catch (RuntimeException e) {
			if (trns != null)
				trns.rollback();
			status = false;
			throw e;
		} finally {
			session.close();
		}
		return status;
	}
}
