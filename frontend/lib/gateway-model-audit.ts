type ModelAuditRecord = {
  model_name: string
  requested_model_name?: string
  upstream_response_model?: string
  upstream_model_mismatch?: boolean
}

export function gatewayModelAudit(record: ModelAuditRecord) {
  const sent = record.model_name?.trim() || ""
  const response = record.upstream_response_model?.trim() || ""
  // Compare the dispatched ID, not the client's pre-mapping alias or billing
  // model. A missing response declaration is unknown, not a mismatch.
  return { sent, response, differs: Boolean(sent && response && sent !== response) }
}
