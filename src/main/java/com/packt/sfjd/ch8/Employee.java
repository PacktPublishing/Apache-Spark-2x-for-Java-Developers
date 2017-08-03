package com.packt.sfjd.ch8;

import java.io.Serializable;

public class Employee implements Serializable {

	/**
		 * 
		 */
	private static final long serialVersionUID = 1L;

	

	private int empId;
	private String empName;
	private String job;
	private String manager;
	private String hiredate;
	private double salary;
	private String comm;
	private double deptNo;
	
	
	public Employee() {
	}
	public Employee(int empId, String empName, String job, String manager, String hiredate, int salary, String comm,
			int deptNo) {
		super();
		this.empId = empId;
		this.empName = empName;
		this.job = job;
		this.manager = manager;
		this.hiredate = hiredate;
		this.salary = salary;
		this.comm = comm;
		this.deptNo = deptNo;
	}

	
	
	public Employee(int empId, String empName) {
		super();
		this.empId = empId;
		this.empName = empName;
	}



	public int getEmpId() {
		return empId;
	}

	public void setEmpId(int empId) {
		this.empId = empId;
	}

	public String getEmpName() {
		return empName;
	}

	public void setEmpName(String empName) {
		this.empName = empName;
	}

	public String getJob() {
		return job;
	}

	public void setJob(String job) {
		this.job = job;
	}

	public String getManager() {
		return manager;
	}

	public void setManager(String manager) {
		this.manager = manager;
	}

	public String getHiredate() {
		return hiredate;
	}

	public void setHiredate(String hiredate) {
		this.hiredate = hiredate;
	}

	public double getSalary() {
		return salary;
	}

	public void setSalary(double salary) {
		this.salary = salary;
	}

	public String getComm() {
		return comm;
	}

	public void setComm(String comm) {
		this.comm = comm;
	}

	public double getDeptNo() {
		return deptNo;
	}

	public void setDeptNo(double deptNo) {
		this.deptNo = deptNo;
	}

}
