package com.meesho.prismdatabricksapi.repositories;

import com.meesho.prismdatabricksapi.entities.SCIMUser;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import javax.transaction.Transactional;
import java.util.*;

@Repository
public interface SCIMUserRepo extends JpaRepository<SCIMUser,String> {
    Optional<SCIMUser> findById(String application_id);

    @Transactional
    @Query(value="select distinct service_principal_id from prod.scim_user where display_name = ?1 ",nativeQuery = true)
    List getSPNIDByDisplay(String display_name);

    @Transactional
    @Query(value="select distinct spn_token from prod.scim_user where display_name = ?1 ",nativeQuery = true)
    String getSPNTokenByDisplay(String display_name);

    @Transactional
    @Query(value="SELECT count(*) from prod.scim_user where  display_name=?1 and owner_email=?2 ",nativeQuery = true)
    Integer checkUserExists(String display_name,String owner_email);


    @Transactional
    @Query(value="select distinct service_principal_id from prod.scim_user where application_id = ?1 ",nativeQuery = true)
    List getSPNIDByAppID(String application_id);


    @Transactional
    @Modifying
    @Query(value="delete from prod.scim_user where service_principal_id in (?1) ",nativeQuery = true)
    int deleteSPNByID(List service_principal_id);


    @Transactional
    @Modifying
    @Query(value="update prod.scim_user set token_id = ?1, spn_token= ?2, token_expiry_time=?3,token_creation_time=?4,token_owner=?5, owner_id=?6 where application_id = ?7",nativeQuery = true)
    int updateServicePrincipalToken(String token_id, String spn_token, Date token_expiry_time, Date token_creation_time, String token_owner,String owner_id, String application_id);


    @Transactional
    @Modifying()
    @Query(value="update prod.scim_user set display_name = ?1, owner_email= ?2, service_principal_id=?3,active=?4,group_name=?5,group_id=?6 where application_id = ?7 ",nativeQuery = true)
    int updateSPN(String display_name, String owner_email, String owner_id, Boolean active, String group_name, String group_id, String application_id );


}
