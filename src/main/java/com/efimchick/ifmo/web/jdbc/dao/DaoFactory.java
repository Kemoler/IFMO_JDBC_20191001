package com.efimchick.ifmo.web.jdbc.dao;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class DaoFactory {
    public EmployeeDao employeeDAO() {
        return new EmployeeDao() {
            @Override
            public List<Employee> getByDepartment(Department department) {
                String query = "select * from employee where department=?";
                try {
                    PreparedStatement preparedStatement = ConnectionSource.instance().createConnection().prepareStatement(query);
                    preparedStatement.setInt(1, department.getId().intValue());
                    ResultSet resultSet = getRSbyPrepared(preparedStatement);
                    return getEmpListFromRS(resultSet);
                } catch (SQLException exception) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                String query = "select * from employee where manager=?";
                try {
                    PreparedStatement preparedStatement = ConnectionSource.instance().createConnection().prepareStatement(query);
                    preparedStatement.setInt(1, employee.getId().intValue());
                    ResultSet resultSet = getRSbyPrepared(preparedStatement);
                    return getEmpListFromRS(resultSet);
                } catch (SQLException exception) {
                    return null;
                }
            }

            @Override
            public Optional<Employee> getById(BigInteger Id) {
                String query = "select * from employee where id=?";
                try {
                    PreparedStatement preparedStatement = ConnectionSource.instance().createConnection().prepareStatement(query);
                    preparedStatement.setInt(1, Id.intValue());
                    ResultSet resultSet = getRSbyPrepared(preparedStatement);
                    if (resultSet.next()) {
                        return Optional.of(getEmp(resultSet));
                    }
                } catch (SQLException exception) {
                    return null;
                }
                return Optional.empty();
            }

            @Override
            public List<Employee> getAll() {
                return getEmpListFromRS(getRS("select * from employee"));
            }

            @Override
            public Employee save(Employee employee) {
                String query = "insert into employee values (?,?,?,?,?,?,?,?,?)";
                try {
                    PreparedStatement preparedStatement = ConnectionSource.instance().createConnection().prepareStatement(query);
                    preparedStatement.setInt(1, employee.getId().intValue());
                    preparedStatement.setString(2, employee.getFullName().getFirstName());
                    preparedStatement.setString(3, employee.getFullName().getLastName());
                    preparedStatement.setString(4, employee.getFullName().getMiddleName());
                    preparedStatement.setString(5, employee.getPosition().toString());
                    preparedStatement.setInt(6, employee.getManagerId().intValue());
                    preparedStatement.setDate(7, Date.valueOf(employee.getHired()));
                    preparedStatement.setDouble(8, employee.getSalary().doubleValue());
                    preparedStatement.setInt(9, employee.getDepartmentId().intValue());
                    preparedStatement.executeUpdate();
                    return employee;
                } catch (SQLException exception) {
                    exception.printStackTrace();
                    return null;
                }
            }

            @Override
            public void delete(Employee employee) {
                String query = "delete from employee where id=?";
                try {
                    PreparedStatement preparedStatement = ConnectionSource.instance().createConnection().prepareStatement(query);
                    preparedStatement.setInt(1, employee.getId().intValue());
                    preparedStatement.executeUpdate();
                } catch (SQLException exception) {
                    exception.printStackTrace();
                }
            }
        };
    }

    public DepartmentDao departmentDAO() {
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger Id) {
                String query = "select * from department where id=?";
                try {
                    PreparedStatement preparedStatement = ConnectionSource.instance().createConnection().prepareStatement(query);
                    preparedStatement.setInt(1, Id.intValue());
                    ResultSet resultSet = getRSbyPrepared(preparedStatement);
                    if (resultSet.next()) {
                        return Optional.of(getDep(resultSet));
                    }
                } catch (SQLException exception) {return Optional.empty();}
                return Optional.empty();
            }

            @Override
            public List<Department> getAll() {
                return getDepListFromRS(Objects.requireNonNull(getRS("select * from department")));
            }

            @Override
            public Department save(Department department) {
                String query;
                PreparedStatement preparedStatement;
                int idOrder;
                int locationOrder;
                try {
                    if (getById(department.getId()).isPresent()) {
                        query = "update department set location=?, name=? where id=?";
                        idOrder = 3;
                        locationOrder = 1;
                    } else {
                        query = "insert into department values (?, ?, ?)";
                        idOrder = 1;
                        locationOrder = 3;
                    }
                    preparedStatement = ConnectionSource.instance().createConnection().prepareStatement(query);
                    preparedStatement.setInt(idOrder, department.getId().intValue());
                    preparedStatement.setString(2, department.getName());
                    preparedStatement.setString(locationOrder, department.getLocation());
                    preparedStatement.executeUpdate();
                    return department;
                } catch (SQLException exception) {
                    return null;
                }
            }

            @Override
            public void delete(Department department) {
                String query = "delete from department where id=?";
                try {
                   PreparedStatement preparedStatement = ConnectionSource.instance().createConnection().prepareStatement(query);
                   preparedStatement.setString(1, department.getId().toString());
                   preparedStatement.executeUpdate();
                } catch (SQLException exception) {
                    exception.printStackTrace();
                }
            }
        };
    }

    private ResultSet getRS(String query) {
        try {
            return ConnectionSource.instance().createConnection().createStatement().executeQuery(query);
        } catch (SQLException exception) {
            return null;
        }
    }

    private ResultSet getRSbyPrepared(PreparedStatement preparedStatement) throws SQLException {
        return preparedStatement.executeQuery();
    }

    private Department getDep(ResultSet resultSet) throws SQLException {
        return new Department(BigInteger.valueOf(resultSet.getInt("id")), resultSet.getString("name"), resultSet.getString("location"));
    }

    private Employee getEmp(ResultSet resultSet) throws SQLException {
        BigInteger manager = (resultSet.getObject("manager") != null) ? BigInteger.valueOf(resultSet.getInt("manager")) : BigInteger.valueOf(0);
        BigInteger department = (resultSet.getObject("department") != null) ? BigInteger.valueOf(resultSet.getInt("department")) : BigInteger.valueOf(0);
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
                            manager, department
                            );
    }

    private List<Employee> getEmpListFromRS(ResultSet resultSet) {
        List<Employee> employeeList = new ArrayList<>();
        try {
            while (resultSet.next()) {
                employeeList.add(getEmp(resultSet));
            }
            return employeeList;
        } catch (SQLException exception) {
            return null;
        }
    }

    private List<Department> getDepListFromRS(ResultSet resultSet) {
        List<Department> departmentList = new ArrayList<>();
        try {
            while (resultSet.next()) {
                departmentList.add(getDep(resultSet));
            }
            return departmentList;
        } catch (SQLException exception) {
            return null;
        }
    }


}