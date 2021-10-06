package com.p3.archon.dboperations.dao;

import org.hibernate.Session;
import org.hibernate.Transaction;

import com.p3.archon.core.ArchonDBUtils;
import com.p3.archon.dboperations.dbmodel.ErtJobDetails;
import com.p3.archon.dboperations.dbmodel.enums.ErtJobStatus;

public class ErtJobDetailsDAO {

	@SuppressWarnings({ "unused" })
	public ErtJobDetails getRecordsOfJob(String jobId) {
		ErtJobDetails jobDetail = new ErtJobDetails();
		Transaction trns = null;
		Session session = ArchonDBUtils.getSessionFactory().openSession();
		try {
			trns = session.beginTransaction();
			jobDetail = (ErtJobDetails) session.createQuery("from ErtJobDetails where JOB_ID='" + jobId + "'").list()
					.get(0);
		} catch (RuntimeException e) {
			e.printStackTrace();
		} finally {
			session.flush();
			session.close();
		}
		return jobDetail;
	}

	public boolean updateJobStatus(String jobId, ErtJobStatus jobStatus) {
		Transaction trns = null;
		boolean status = false;
		Session session = ArchonDBUtils.getSessionFactory().openSession();
		try {
			trns = session.beginTransaction();
			ErtJobDetails wd = (ErtJobDetails) session.get(ErtJobDetails.class, jobId);
			wd.setJobStatus(jobStatus);
			session.update(wd);
			session.getTransaction().commit();
			status = true;
		} catch (RuntimeException e) {
			if (trns != null) {
				trns.rollback();
			}
			status = false;
		} finally {
			session.close();
		}
		return status;
	}

}
