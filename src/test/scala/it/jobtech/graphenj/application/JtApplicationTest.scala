package it.jobtech.graphenj.application

import it.jobtech.graphenj.core.{ JtDqJob, JtEtlDqJob, JtEtlJob }
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JtApplicationTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  test("instantiate a JtDqJob using JtApplication") {
    JtApplication.init("dq_job_config.json")
    JtApplication.job.isInstanceOf[JtDqJob] shouldBe true
    JtApplication.close()
  }

  test("instantiate a JtEtlJob using JtApplication") {
    JtApplication.init("etl_job_config.json")
    JtApplication.job.isInstanceOf[JtEtlJob] shouldBe true
    JtApplication.close()
  }

  test("instantiate a JtEtlDqJob using JtApplication") {
    JtApplication.init("etl_dq_job_config.json")
    JtApplication.job.isInstanceOf[JtEtlDqJob] shouldBe true
    JtApplication.close()
  }

}
