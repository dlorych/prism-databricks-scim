package com.meesho.prismdatabricksapi.repositories;

import com.meesho.prismdatabricksapi.entities.SCIMUser;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SCIMUserRepo extends CrudRepository<SCIMUser, Integer> {



}