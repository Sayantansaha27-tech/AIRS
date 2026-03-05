import { IncidentDetail } from '@/components/incident-detail';

export default function IncidentPage({ params }: { params: { id: string } }) {
  return <IncidentDetail incidentId={params.id} />;
}
