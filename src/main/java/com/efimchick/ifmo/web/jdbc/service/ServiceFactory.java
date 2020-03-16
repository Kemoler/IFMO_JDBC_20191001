package com.efimchick.ifmo.web.jdbc.service;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class ServiceFactory {
    private static final String  queryWithManager = "select e.*, m.id mid, m.firstname mfirstName, m.lastName mlastName, " +
                                                    "m.middleName mmiddleName, m.position mposition, m.hireDate mhiredate, " +
                                                    "m.salary msalary, m.department mdepartment, m.manager mmanager, d.name dname, d.location dlocation, " +
                                                    "md.name mdname, md.location mdlocation " +
                                                    "from employee e " +
                                                    "left join employee m on e.manager=m.id " +
                                                    "left join department d on e.department=d.id " +
                                                    "left join department md on m.department=md.id ";
    private static final String queryWithChain = "select e.*, d.name dname, d.location dlocation from employee e left join department d on e.department=d.id";

    public EmployeeService employeeService(){
        return new EmployeeService() {
            @Override
            public List<Employee> getAllSortByHireDate(Paging paging) {
                String query = queryWithManager + "order by e.hireDate limit " + paging.itemPerPage + " offset " + ((paging.page-1)*paging.itemPerPage);
                return getEmployeesByQuery(query, false);
            }

            @Override
            public List<Employee> getAllSortByLastname(Paging paging) {
                String query = queryWithManager + "order by e.lastName limit " + paging.itemPerPage + " offset " + ((paging.page-1)*paging.itemPerPage);
                return getEmployeesByQuery(query, false);
            }

            @Override
            public List<Employee> getAllSortBySalary(Paging paging) {
                String query = queryWithManager + "order by e.salary limit " + paging.itemPerPage + " offset " + ((paging.page-1)*paging.itemPerPage);
                return getEmployeesByQuery(query, false);
            }

            @Override
            public List<Employee> getAllSortByDepartmentNameAndLastname(Paging paging) {
                String query = queryWithManager + "order by d.name, e.lastName limit " + paging.itemPerPage + " offset " + ((paging.page-1)*paging.itemPerPage);
                return getEmployeesByQuery(query, false);
            }

            @Override
            public List<Employee> getByDepartmentSortByHireDate(Department department, Paging paging) {
                String query = queryWithManager + "where e.department=" + department.getId().toString() + " order by hireDate limit " + paging.itemPerPage + " offset " + ((paging.page-1)*paging.itemPerPage);
                return getEmployeesByQuery(query, false);
            }

            @Override
            public List<Employee> getByDepartmentSortBySalary(Department department, Paging paging) {
                String query = queryWithManager + "where e.department=" + department.getId().toString() + " order by salary limit " + paging.itemPerPage + " offset " + ((paging.page-1)*paging.itemPerPage);
                return getEmployeesByQuery(query, false);
            }

            @Override
            public List<Employee> getByDepartmentSortByLastname(Department department, Paging paging) {
                String query = queryWithManager + "where e.department=" + department.getId().toString() + " order by lastName limit " + paging.itemPerPage + " offset " + ((paging.page-1)*paging.itemPerPage);
                return getEmployeesByQuery(query, false);
            }

            @Override
            public List<Employee> getByManagerSortByLastname(Employee manager, Paging paging) {
                String query = queryWithManager + "where e.manager=" + manager.getId().toString() + " order by lastName limit " + paging.itemPerPage + " offset " + ((paging.page-1)*paging.itemPerPage);
                return getEmployeesByQuery(query, false);
            }

            @Override
            public List<Employee> getByManagerSortByHireDate(Employee manager, Paging paging) {
                String query = queryWithManager + "where e.manager=" + manager.getId().toString() + " order by hireDate limit " + paging.itemPerPage + " offset " + ((paging.page-1)*paging.itemPerPage);
                return getEmployeesByQuery(query, false);
            }

            @Override
            public List<Employee> getByManagerSortBySalary(Employee manager, Paging paging) {
                String query = queryWithManager + "where e.manager=" + manager.getId().toString() + " order by salary limit " + paging.itemPerPage + " offset " + ((paging.page-1)*paging.itemPerPage);
                return getEmployeesByQuery(query, false);
            }

            @Override
            public Employee getWithDepartmentAndFullManagerChain(Employee employee) {
                String query = queryWithChain + " where e.id=" + employee.getId().toString();
                System.out.println(query);
                try {
                    ResultSet resultSet = ConnectionSource.instance().createConnection().createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE).executeQuery(query);
                    if (resultSet.next()) {
                        return getEmployeeWithChain(resultSet);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public Employee getTopNthBySalaryByDepartment(int salaryRank, Department department) {
                String query = queryWithManager + "where e.department=" + department.getId().toString() + " order by salary desc";
                return (getEmployeesByQuery(query, false)).get(salaryRank-1);
            }
        };
    }

    //private List<Employee>
    private Employee getEmployeeWithManager(ResultSet resultSet) throws SQLException {
        Employee manager = (resultSet.getObject("manager")!=null) ? getManager(resultSet) : null;
        Department department = (resultSet.getObject("department") == null) ? null : new Department(
            BigInteger.valueOf(resultSet.getInt("department")), resultSet.getString("dname"), resultSet.getString("dlocation")
        );
        return getEmployee(resultSet, manager, department);
    }

    private Employee getEmployeeWithChain(ResultSet resultSet) throws SQLException {
        return getEmployeeForChain(resultSet);
    }

    private Employee getEmployeeForChain(ResultSet resultSet) throws SQLException {
        Department department = (resultSet.getObject("department") == null) ? null : new Department(
                BigInteger.valueOf(resultSet.getInt("department")), resultSet.getString("dname"), resultSet.getString("dlocation"));
        return new Employee(
                BigInteger.valueOf(resultSet.getInt("id")),
                new FullName(
                        resultSet.getString("firstName"),
                        resultSet.getString("lastName"),
                        resultSet.getString("middleName")
                ),
                Position.valueOf(resultSet.getString("position")),
                LocalDate.parse(resultSet.getString("hireDate")),
                resultSet.getBigDecimal("salary"),
                (resultSet.getObject("manager")!=null) ? getManagerWithChain(BigInteger.valueOf(resultSet.getInt("manager"))) : null, department
        );
    }

    private Employee getEmployee(ResultSet resultSet, Employee manager, Department department) throws SQLException {
        return new Employee(
                BigInteger.valueOf(resultSet.getInt("id")),
                new FullName(
                        resultSet.getString("firstName"),
                        resultSet.getString("lastName"),
                        resultSet.getString("middleName")
                ),
                Position.valueOf(resultSet.getString("position")),
                LocalDate.parse(resultSet.getString("hireDate")),
                resultSet.getBigDecimal("salary"),
                manager,
                department
        );
    }
    private Employee getManager(ResultSet resultSet) throws SQLException {
        Department department = (resultSet.getObject("mdepartment") == null) ? null : new Department(
                BigInteger.valueOf(resultSet.getInt("mdepartment")), resultSet.getString("mdname"), resultSet.getString("mdlocation")
        );
        return new Employee(
                BigInteger.valueOf(resultSet.getInt("manager")),
                new FullName(
                        resultSet.getString("mfirstName"),
                        resultSet.getString("mlastName"),
                        resultSet.getString("mmiddleName")
                ),
                Position.valueOf(resultSet.getString("mposition")),
                LocalDate.parse(resultSet.getString("mhireDate")),
                resultSet.getBigDecimal("msalary"),
                null,
                department
        );
    }

    private List<Employee> getEmployeesByQuery(String query, boolean chain) {
        List<Employee> employees = new ArrayList<>();
        try {
            ResultSet resultSet = ConnectionSource.instance().createConnection().createStatement().executeQuery(query);
            while (resultSet.next()) {
                if (!chain) {
                    employees.add(getEmployeeWithManager(resultSet));
                } else {
                    employees.add(getEmployeeWithChain(resultSet));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        return employees;
    }

    private Employee getManagerWithChain(BigInteger manager) throws SQLException {
        ResultSet resultSet = ConnectionSource.instance().createConnection().createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE).executeQuery((queryWithChain + " where e.id=" + manager.toString()));
        if (resultSet.next()) {
            return getEmployeeForChain(resultSet);
        }
        return null;
    }

}
