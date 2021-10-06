package com.p3.archon.dboperations.dao;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;

import com.p3.archon.core.ArchonDBUtils;
import com.p3.archon.dboperations.dbmodel.ExtractionLocation;
import com.p3.archon.dboperations.dbmodel.RowId;

public class ExtractionLocationDAO {

	public boolean addRecord(ExtractionLocation jd) {
		Transaction trns = null;
		boolean status = false;
		Session session = ArchonDBUtils.getSessionFactory().openSession();
		try {
			trns = session.beginTransaction();
			session.saveOrUpdate(jd);
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

	@SuppressWarnings({ "deprecation", "unused" })
	public String getOutputLoc(RowId rid) {
		String loc = null;
		Transaction trns = null;
		Session session = ArchonDBUtils.getSessionFactory().openSession();
		try {
			trns = session.beginTransaction();
			Criteria criteria = session.createCriteria(ExtractionLocation.class);
			criteria.setProjection(Projections.projectionList().add(Projections.property("loc")))
					.add(Restrictions.eq("rowId.jobId", rid.getJobId()))
					.add(Restrictions.eq("rowId.runId", rid.getRunId()))
					.add(Restrictions.eq("rowId.jobAttempt", rid.getJobAttempt()));
			loc = (String) criteria.list().get(0);
		} catch (RuntimeException e) {
			e.printStackTrace();
		} finally {
			session.flush();
			session.close();
		}
		return loc;
	}

}
