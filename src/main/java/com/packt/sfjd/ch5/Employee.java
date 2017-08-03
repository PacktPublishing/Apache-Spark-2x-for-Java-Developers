package com.packt.sfjd.ch5;

import java.io.Serializable;

public class Employee implements Serializable{
		private Integer empid;
		private String emp_name;
		private String emp_dept;
		/*public Employee(Integer empid, String emp_name, String emp_dept) {
			super();
			this.empid = empid;
			this.emp_name = emp_name;
			this.emp_dept = emp_dept;
		}*/
		
		public String toString() {
			return "Employee [empid=" + empid + ", emp_name=" + emp_name + ", emp_dept=" + emp_dept + "]";
		}

		
		public Integer getEmpid() {
			return empid;
		}
		

		public void setEmpid(Integer empid) {
			this.empid = empid;
		}
		public String getEmp_name() {
			return emp_name;
		}
		public void setEmp_name(String emp_name) {
			this.emp_name = emp_name;
		}
		public String getEmp_dept() {
			return emp_dept;
		}
		public void setEmp_dept(String emp_dept) {
			this.emp_dept = emp_dept;
		}
		
	}