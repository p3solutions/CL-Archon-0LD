package com.p3.archon.dboperations.dao;

import org.hibernate.Session;
import org.hibernate.Transaction;

import com.p3.archon.core.ArchonDBUtils;
import com.p3.archon.dboperations.dbmodel.LotusNotesSessions;

public class LotusNotesSessionsDAO {

	public boolean addRecord(LotusNotesSessions ms) {
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
		} finally {
			session.close();
		}
		return status;
	}
}
