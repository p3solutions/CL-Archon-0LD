package com.p3.archon.dboperations.dao;

import org.hibernate.Session;
import org.hibernate.Transaction;

import com.p3.archon.core.ArchonDBUtils;
import com.p3.archon.dboperations.dbmodel.WFDetails;

@SuppressWarnings({ "unused" })
public class WFDetailsDAO {

	public WFDetails getRecordById(String wfId) {
		WFDetails wfdetail = new WFDetails();
		boolean status = false;
		Transaction trns = null;
		Session session = ArchonDBUtils.getSessionFactory().openSession();
		try {
			trns = session.beginTransaction();
			wfdetail = (WFDetails) session.createQuery("from WFDetails where WF_ID='" + wfId + "'").list().get(0);
		} catch (RuntimeException e) {
			e.printStackTrace();
		} finally {
			session.flush();
			session.close();
		}
		return wfdetail;
	}

	public boolean updateRecord(WFDetails wfdetail) {
		Transaction trns = null;
		boolean status = false;
		Session session = ArchonDBUtils.getSessionFactory().openSession();
		try {
			trns = session.beginTransaction();
			session.update(wfdetail);
			session.getTransaction().commit();
			status = true;
		} catch (RuntimeException e) {
			if (trns != null) {
				trns.rollback();
			}
			status = false;
			throw e;
		} finally {
			session.close();
		}
		return status;
	}
}
