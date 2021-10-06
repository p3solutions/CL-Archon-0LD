package com.p3.archon.dboperations.dao;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Session;
import org.hibernate.Transaction;

import com.p3.archon.core.ArchonDBUtils;
import com.p3.archon.dboperations.dbmodel.As400MetalyzerSessions;

public class As400MetalyzerSessionsDAO {

	public boolean addRecord(As400MetalyzerSessions ms) {
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

	public boolean updateRecord(As400MetalyzerSessions record) {
		Transaction trns = null;
		boolean status = false;
		Session session = ArchonDBUtils.getSessionFactory().openSession();
		try {
			trns = session.beginTransaction();
			session.saveOrUpdate(record);
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
	
	@SuppressWarnings("unchecked")
	public List<As400MetalyzerSessions> getBaseRecords(String host, String port, String lib, String username, String userId) {
		List<As400MetalyzerSessions> maSessions = new ArrayList<As400MetalyzerSessions>();
		Transaction trns = null;
		Session session = ArchonDBUtils.getSessionFactory().openSession();
		try {
			trns = session.beginTransaction();
			maSessions = session.createQuery("from As400MetalyzerSessions where USER_ID = '" + userId
					+ "' and HOST = '" + host + ((port != null && !port.isEmpty()) ? ("' and PORT = '" + port) : "")
					+ ((lib != null && !lib.isEmpty()) ? ("' and LIB = '" + lib) : "") + "' and USER_NAME = '"
					+ username + "' and BASE_IND = '" + 1 + "'").list();
		} catch (RuntimeException e) {
			if (trns != null) {
				trns.rollback();
			}
			throw e;
		} finally {
			session.close();
		}
		return maSessions;
	}
}
