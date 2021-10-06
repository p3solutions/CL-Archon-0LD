package com.p3.archon.dboperations.dao;

import org.hibernate.Session;
import org.hibernate.Transaction;

import com.p3.archon.core.ArchonDBUtils;
import com.p3.archon.dboperations.dbmodel.FlatFileSessions;

public class FlatFileSessionsDAO {

	public boolean updateRecord(FlatFileSessions record) {
		Transaction trns = null;
		boolean status = false;
		Session session = ArchonDBUtils.getSessionFactory().openSession();
		try {
			trns = session.beginTransaction();
			FlatFileSessions ud = updateJobDetails(
					(FlatFileSessions) session.get(FlatFileSessions.class, record.getJobId()), record);
			session.update(ud);
			session.getTransaction().commit();
			status = true;
		} catch (RuntimeException e) {
			if (trns != null) {
				trns.rollback();
			}
			status = false;
			e.printStackTrace();
		} finally {
			session.close();
		}
		return status;
	}

	private FlatFileSessions updateJobDetails(FlatFileSessions toupdate, FlatFileSessions updated) {
		toupdate.setJobName(updated.getJobName());
		toupdate.setJobType(updated.getJobType());
		toupdate.setCreatedDt(updated.getCreatedDt());
		toupdate.setCreatedBy(updated.getCreatedBy());
		toupdate.setLastAccessDt(updated.getLastAccessDt());
		toupdate.setLastAccessedBy(updated.getLastAccessedBy());
		toupdate.setJobStatus(updated.getJobStatus());
		toupdate.setSavedJobDetails(updated.getSavedJobDetails());
		toupdate.setInputPathBlob(updated.getInputPathBlob());
		toupdate.setMetadataPathBlob(updated.getMetadataPathBlob());
		return toupdate;
	}

	public FlatFileSessions getRecordsOfJob(String jobId) {
		FlatFileSessions jobDetail = new FlatFileSessions();
		Transaction trns = null;
		Session session = ArchonDBUtils.getSessionFactory().openSession();
		try {
			trns = session.beginTransaction();
			jobDetail = (FlatFileSessions) session.createQuery("from FlatFileSessions where JOB_ID='" + jobId + "'")
					.list().get(0);
		} catch (RuntimeException e) {
			e.printStackTrace();
		} finally {
			session.flush();
			session.close();
		}
		return jobDetail;
	}
}
