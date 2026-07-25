package edu.anant.dto;



import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Employee {

    private Integer id;

    private String firstName;

    private String lastName;

    private String email;

    private String department;

    private String designation;

    private Double salary;



}