use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PrincipalId(String);

impl PrincipalId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(String);

impl SessionId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Owner,
    Viewer,
    Controller,
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    ViewStream,
    ViewInputLogs,
    SendKey,
    ReadMemory,
    AdminLifecycle,
}

impl Role {
    pub fn allows(self, permission: Permission) -> bool {
        match self {
            Role::Admin => true,
            Role::Owner => !matches!(permission, Permission::AdminLifecycle),
            Role::Controller => matches!(
                permission,
                Permission::ViewStream
                    | Permission::ViewInputLogs
                    | Permission::SendKey
                    | Permission::ReadMemory
            ),
            Role::Viewer => matches!(
                permission,
                Permission::ViewStream | Permission::ViewInputLogs
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    pub id: Uuid,
    pub principal_id: PrincipalId,
    pub session_id: SessionId,
    pub role: Role,
    pub revoked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipalToken {
    pub token: String,
    pub principal_id: PrincipalId,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("principal token is invalid")]
    InvalidToken,
    #[error("principal has no active grant for session")]
    MissingGrant,
    #[error("principal is not permitted to access this session resource")]
    Forbidden,
}

#[derive(Debug, Default, Clone)]
pub struct AclService {
    token_hash_to_principal: HashMap<String, PrincipalId>,
    grants: HashMap<(PrincipalId, SessionId), Grant>,
}

impl AclService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn issue_principal_token(&mut self, principal_id: PrincipalId) -> PrincipalToken {
        let token = generate_token();
        self.register_principal_token(principal_id.clone(), &token);
        PrincipalToken {
            token,
            principal_id,
        }
    }

    pub fn register_principal_token(&mut self, principal_id: PrincipalId, token: &str) {
        self.token_hash_to_principal
            .insert(hash_token(token), principal_id);
    }

    pub fn grant(&mut self, principal_id: PrincipalId, session_id: SessionId, role: Role) -> Grant {
        let grant = Grant {
            id: Uuid::new_v4(),
            principal_id: principal_id.clone(),
            session_id: session_id.clone(),
            role,
            revoked: false,
        };
        self.grants
            .insert((principal_id, session_id), grant.clone());
        grant
    }

    pub fn revoke(&mut self, principal_id: &PrincipalId, session_id: &SessionId) -> Option<Grant> {
        let grant = self
            .grants
            .get_mut(&(principal_id.clone(), session_id.clone()))?;
        grant.revoked = true;
        Some(grant.clone())
    }

    pub fn resolve_token(&self, token: &str) -> Result<PrincipalId, AuthError> {
        self.token_hash_to_principal
            .get(&hash_token(token))
            .cloned()
            .ok_or(AuthError::InvalidToken)
    }

    pub fn check_token(
        &self,
        token: &str,
        session_id: &SessionId,
        permission: Permission,
    ) -> Result<PrincipalId, AuthError> {
        let principal = self.resolve_token(token)?;
        self.check_principal(&principal, session_id, permission)?;
        Ok(principal)
    }

    pub fn check_principal(
        &self,
        principal_id: &PrincipalId,
        session_id: &SessionId,
        permission: Permission,
    ) -> Result<(), AuthError> {
        let grant = self
            .grants
            .get(&(principal_id.clone(), session_id.clone()))
            .ok_or(AuthError::MissingGrant)?;
        if grant.revoked {
            return Err(AuthError::MissingGrant);
        }
        if grant.role.allows(permission) {
            Ok(())
        } else {
            Err(AuthError::Forbidden)
        }
    }
}

fn generate_token() -> String {
    let mut bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hash_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_matrix_matches_session_acl_contract() {
        assert!(Role::Viewer.allows(Permission::ViewStream));
        assert!(Role::Viewer.allows(Permission::ViewInputLogs));
        assert!(!Role::Viewer.allows(Permission::SendKey));
        assert!(!Role::Viewer.allows(Permission::ReadMemory));
        assert!(Role::Controller.allows(Permission::SendKey));
        assert!(Role::Controller.allows(Permission::ReadMemory));
        assert!(!Role::Controller.allows(Permission::AdminLifecycle));
        assert!(Role::Admin.allows(Permission::AdminLifecycle));
    }

    #[test]
    fn acl_is_session_scoped_and_deny_by_default() {
        let mut acl = AclService::new();
        let principal = PrincipalId::new("alice");
        let token = acl.issue_principal_token(principal.clone()).token;
        let session_a = SessionId::new("session-a");
        let session_b = SessionId::new("session-b");
        acl.grant(principal.clone(), session_a.clone(), Role::Controller);

        assert_eq!(
            acl.check_token(&token, &session_a, Permission::SendKey),
            Ok(principal.clone())
        );
        assert_eq!(
            acl.check_token(&token, &session_b, Permission::ViewStream),
            Err(AuthError::MissingGrant)
        );
        assert_eq!(
            acl.check_token("bad", &session_a, Permission::SendKey),
            Err(AuthError::InvalidToken)
        );
    }

    #[test]
    fn revoked_grant_fails_closed() {
        let mut acl = AclService::new();
        let principal = PrincipalId::new("alice");
        let token = acl.issue_principal_token(principal.clone()).token;
        let session = SessionId::new("session-a");
        acl.grant(principal.clone(), session.clone(), Role::Owner);
        acl.revoke(&principal, &session);

        assert_eq!(
            acl.check_token(&token, &session, Permission::ViewStream),
            Err(AuthError::MissingGrant)
        );
    }
}
