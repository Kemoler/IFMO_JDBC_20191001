package com.efimchick.ifmo.web.jdbc;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

public class SetMapperFactory {

    public SetMapper<Set<Employee>> employeesSetMapper() {
        return resultSet -> {
            Set<Employee> employeeSet = new HashSet<>();
            try {
                while (resultSet.next()) {
                    employeeSet.add(getEmployee(resultSet));
                }
                return employeeSet;
            } catch (SQLException exception) {
                return null;
            }
        };
    }

    private Employee getEmployee(ResultSet resultSet) {
        try {
            return new Employee(
                    new BigInteger(resultSet.getString("id")),
                    new FullName(
                            resultSet.getString("firstname"),
                            resultSet.getString("lastname"),
                            resultSet.getString("middlename")
                    ),
                    Position.valueOf(resultSet.getString("position")),
                    LocalDate.parse(resultSet.getString("hiredate")),
                    resultSet.getBigDecimal("salary"),
                    getManager(resultSet)
            );
        } catch (SQLException exception) {
            return null;
        }
    }

    private Employee getManager(ResultSet resultSet) throws SQLException {
        int resultSetRow = resultSet.getRow();

        if (resultSet.getInt("manager") == 0) {
            return null;
        } else {
            int managerId = resultSet.getInt("manager");
            Employee manager = null;
            if (!resultSet.isBeforeFirst()) resultSet.beforeFirst();
            while (resultSet.next()) {
                if (resultSet.getInt("id") == managerId) {
                    manager = getEmployee(resultSet);
                }
            }
            resultSet.absolute(resultSetRow);
            return manager;
        }
    }
}
