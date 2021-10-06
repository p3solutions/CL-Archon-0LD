package com.p3.archon.dboperations.dao;

import org.hibernate.Session;
import org.hibernate.Transaction;

import com.p3.archon.core.ArchonDBUtils;
import com.p3.archon.dboperations.dbmodel.JobDetails;
import com.p3.archon.dboperations.dbmodel.RowId;


public class JobDetailsDAO {
	public boolean addRecord(JobDetails jd) {
		Transaction trns = null;
		boolean status = false;
		Session session = ArchonDBUtils.getSessionFactory().openSession();
		try {
			trns = session.beginTransaction();
			session.save(jd);
			session.getTransaction().commit();
			status = true;
		} catch (RuntimeException e) {
			if (trns != null)
				trns.rollback();
			status = false;
			e.printStackTrace();
		} finally {
			session.close();
		}
		return status;
	}

	public JobDetails getJobRecord(RowId rowId) {
		JobDetails jd = null;
		@SuppressWarnings("unused")
		Transaction trns = null;
		Session session = ArchonDBUtils.getSessionFactory().openSession();
		try {
			trns = session.beginTransaction();
			jd = (JobDetails) session.get(JobDetails.class, rowId);
		} catch (RuntimeException e) {
			e.printStackTrace();
		} finally {
			session.flush();
			session.close();
		}
		return jd;
	}

	@SuppressWarnings("unused")
	public JobDetails getRecordsById(RowId rowId) {
		Transaction trns = null;
		Session session = ArchonDBUtils.getSessionFactory().openSession();
		JobDetails jobDetails = new JobDetails();
		try {
			trns = session.beginTransaction();
			jobDetails = (JobDetails) session.get(JobDetails.class, rowId);
		} catch (RuntimeException e) {
			e.printStackTrace();
		} finally {
			session.close();
		}
		return jobDetails;
	}

	public boolean updateRecord(JobDetails jobDetails) {
		Transaction trns = null;
		boolean status = false;
		Session session = ArchonDBUtils.getSessionFactory().openSession();
		try {
			trns = session.beginTransaction();
			JobDetails jd = updateDetails((JobDetails) session.load(JobDetails.class, jobDetails.getRowId()),
					jobDetails);
			session.update(jd);
			session.getTransaction().commit();
			status = true;
		} catch (RuntimeException e) {
			status = false;
			if (trns != null)
				trns.rollback();
			e.printStackTrace();
		} finally {
			session.close();
		}
		return status;
	}
	
	private JobDetails updateDetails(JobDetails toupdate, JobDetails updated) {
		toupdate.setToolName(updated.getToolName());
		toupdate.setToolCategory(updated.getToolCategory());
		toupdate.setJobRunType(updated.getJobRunType());
		toupdate.setJobInterval(updated.getJobInterval());
		toupdate.setJobName(updated.getJobName());
		toupdate.setJobFrequency(updated.getJobFrequency());
		toupdate.setInputDetails(updated.getInputDetails());
		toupdate.setScheduledTime(updated.getScheduledTime());
		toupdate.setStartTime(updated.getStartTime());
		toupdate.setEndTime(updated.getEndTime());
		toupdate.setUserId(updated.getUserId());
		toupdate.setUserEmail(updated.getUserEmail());
		toupdate.setStatus(updated.getStatus());
		toupdate.setJobRetried(updated.getJobRetried());
		toupdate.setMessage(updated.getMessage());
		toupdate.setHandler(updated.getHandler());
		toupdate.setAuditLogZipFile(updated.getAuditLogZipFile());
		toupdate.setMetaBlob(updated.getMetaBlob());
		return toupdate;
	}

}
