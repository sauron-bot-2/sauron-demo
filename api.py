from collections import defaultdict
import datetime
import json
import traceback
import re
import phonenumbers
import logging

from dateutil.relativedelta import relativedelta
from decimal import Decimal
from django.db.models import Q, Prefetch
from django.http import HttpResponse, JsonResponse
from django.http import Http404
from django.views.generic import View
from extensions.rest import CompanyOrEmployeeAuthorizationMixin, ExtendedModelResource, EmployeeOrCompanyAuthorizationMixin, \
	NoAuthorizationMixin, AddCompanyOnCreate, CompanyAuthorizationMixin, ExtendedResource, ExtendedEmployeeToOneField, \
	EmployeeOrCompanyDocumentAuthorizationMixin, FieldModelMixin, EmployeeAuthorizationFiltering, SalesDemoCacheGetMixin, \
	FilterMixin, ConsoleViewAuthorizationMixin, CompanyAdminOpsAuthorizationMixin, EmployerWriteEmployeeReadAuthorizationMixin
from extensions.tastypie.authorization import Authorization
from extensions.utils import formatUSDate, formatMaskedSSN, formatMaskedSalary, validFileLink
from extensions.views import CompanyManagerViewMixin

from company_health_qem.models import Plan, DentalPlan, VisionPlan
from register_company.models import Company, Employee, CompanySettings, EmployeeSettings, EmployeeTerminationAction, \
	EmergencyContacts, EmployeeHrContact, EmployeeSyncDifference, CoverageCancellation, Signature, Partner, \
	StateTax, TerminationSettings, EmpTermOverride, PayrollCompanySettings, ZenefitsContacts, EmployeeBenefitsInfo, \
	CompanyExemption, EmployeeExemption, RequestedEmployeeExemption, EmployeeRelationship
from register_company import exemption_services as ExemptionService
from accounts.decorators import login_required
from django.views.decorators.http import require_POST

# Mobile API
from mobile_api.api import ZenefitsPeopleApiMixin

log = logging.getLogger(__name__)

class EmployeeContactResource(ExtendedModelResource):
	class Meta:
		model = EmployeeContact
		resource_name = "employee_contact"
		filtering = {
			'employee_id': 'exact',
		}
		authorization_filtering = EmployeeAuthorizationFiltering(permissions=['self', 'admin'])

class ManualCompanyResource(ModelResource):
	class Meta:
		queryset = Company.objects.all()
		resource_name = 'manual_company'
		authorization = Authorization()
		always_return_data = True
		include_resource_uri = False
		fields = ['name', 'phone', 'zip']
		limit = 0
		default_format = "application/json"

	def determine_format(self, request):
		"""
		Used to determine the desired format from the request.format
		attribute.
		"""
		if (hasattr(request, 'format') and
				request.format in self._meta.serializer.formats):
			return self._meta.serializer.get_mime_for_format(request.format)
		return "application/json"

	# HACK HACK
	def obj_get(self, request=None, **kwargs):
		identifier = kwargs['pk']
		if (identifier == 'me'):
			try:
				company = Company.fromRequest(request)
				if company.name == 'NewCo':
					company.name = ''
					company.save()
			except Company.DoesNotExist:
				company = Company()
				company.name = ''
				company.save()

			return company

		return None

	def hydrate_zip(self, bundle):
		zipCode = bundle.data['zip']
		if zipCode:
			try:
				zipCode = int(zipCode)
				mapping = zipcountyservice.getCountyForZip(zipCode)
				bundle.obj.state = mapping.state
				bundle.data['state'] = mapping.state
			except:
				bundle.obj.state = ''
				bundle.data['state'] = ''

		return bundle

	def hydrate_phone(self, bundle):
		# replace all non numbers
		phone = bundle.data['phone']
		if phone:
			phone = re.sub(r"[^\d.]+", "", phone)
			bundle.data['phone'] = phone

		return bundle

	def dehydrate(self, bundle):
		# format phone number
		if bundle.obj.phone:
			phone = phonenumbers.parse(bundle.obj.phone, "US")
			bundle.data['phone'] = phonenumbers.format_number(phone, phonenumbers.PhoneNumberFormat.NATIONAL)
		else:
			bundle.data['phone'] = None

		if not bundle.obj.zip:
			bundle.data['zip'] = None

		return bundle

	def apply_authorization_limits(self, request, object_list):
		if request.permission.isMainAdmin:
			return object_list.filter(id=request.company_id)
		return object_list.none()


class EmployeeTerminationActionResource(ExtendedModelResource):
	reportsNewManager_id = ExtendedEmployeeToOneField('reportsNewManager', null=True)
	payrollTerminationReason_id = fields.ToOneField(
		'scraping.api.WfTerminationReasonResource',
		'payrollTerminationReason',
		null=True,
		blank=True)
	payrollAction_id = fields.ToOneField('smp.api.PayrollActionResource', 'payrollAction', null=True, blank=True)
	benefitsEndDate = fields.DateField(attribute='benefitsEndDate', null=True, readonly=True)
	terminationDate = fields.DateField(attribute='terminationDate', null=True, blank=False)
	isCancelable = fields.BooleanField(attribute='isCancelable', readonly=True)

	class Meta:
		# Enabling auto_prefetch would make your api slow as it would prefetch all the related fields defined on the model unnecessarily. https://confluence.inside-zen.com/x/pk81Aw
		auto_prefetch = True
		model = EmployeeTerminationAction
		authorization_filtering = EmployeeAuthorizationFiltering(
			permissions=['self', 'admin', 'manager'])
		resource_name = 'employee_termination_action'
		authorization = Authorization()
		always_return_data = True
		include_resource_uri = True
		limit = 0
		default_format = "application/json"
		excludes = ['emailNewManager', 'newManagerForReports', 'payrollTerminationData', ]

	def dehydrate(self, bundle):
		if bundle.obj.benefitsEndDate:
			bundle.data['benefitsEndDate'] = formatUSDate(bundle.obj.benefitsEndDate)

		if bundle.obj.terminationDate:
			bundle.data['terminationDate'] = formatUSDate(bundle.obj.terminationDate)

		if bundle.obj.terminatedOn:
			bundle.data['terminatedOn'] = formatUSDate(bundle.obj.terminatedOn)

		if bundle.obj.dob:
			bundle.data['dob'] = formatUSDate(bundle.obj.dob)

		if bundle.obj.socialSecurity:
			bundle.data['socialSecurity'] = formatMaskedSSN(bundle.obj.socialSecurity)

		return bundle

	def hydrate(self, bundle):
		if bundle.data['socialSecurity']:
			ssn = bundle.data['socialSecurity']
			ssn = re.sub(r"[^\d.]+", "", ssn)
			if len(ssn) == 9 and isValidSSNOrITIN(ssn):
				bundle.data['socialSecurity'] = ssn
			elif bundle.data.get('id'):
				action = EmployeeTerminationAction.objects.get(id=int(bundle.data.get('id')))
				bundle.data['socialSecurity'] = action.socialSecurity
			else:
				bundle.data['socialSecurity'] = None
		else:
			bundle.data['socialSecurity'] = None

		return bundle

	def obj_create(self, bundle, request=None, **kwargs):
		if not (request.permission.isAdmin or request.permission.canViewSubordinates):
			data = {
				"error_message": "Not Authorized"
			}
			raise ImmediateHttpResponse(response=http.HttpUnauthorized(json.dumps(data)))
		employee = Employee.objects.get(pk=bundle.data['employeeID'], company_id=request.company_id)

		try:
			existingTerminationAction = EmployeeTerminationAction.objects.get(employee=employee)
			#TODO(Gabe): Refactor once we support multiple terminationActions
			if employee.terminationAction.isCanceled:
				employee.terminationAction.delete()
			else:
				log.exception(
					"EmployeeTerminationAction already exists for Employee!",
					extra={
						'employeeID': employee.id,
						'oldTerminationActionID': existingTerminationAction.id,
						'newTerminationActionParams': bundle.obj.__dict__,
					})
				data = {
					"error_message": "Employee already has a termination action."
				}
				raise ImmediateHttpResponse(response=http.HttpUnprocessableEntity(json.dumps(data)))
		except EmployeeTerminationAction.DoesNotExist:
			# Can continue if no termination action exists
			pass

		bundle.obj.employee = employee
		bundle = super(EmployeeTerminationActionResource, self).obj_create(bundle, request, employee=employee)
		bundle.obj.employee.setUser()

		#Log version of term flow used to create this termination action
		termFlowVersion = bundle.data['termFlowVersion'] if 'termFlowVersion' in bundle.data else None
		if termFlowVersion:
			log.info("EmployeeTerminationAction<{}> created with version {}".format(
				bundle.obj.id,
				termFlowVersion))

		return bundle


class CompanySettingsResource(ModelResource):
	openEnrollmentEffectiveDate = fields.CharField(attribute='openEnrollmentEffectiveDate', readonly=True, null=True) # used once
	postACA = fields.BooleanField(attribute='postACA', readonly=True, null=True) # simple
	cobraType = fields.CharField(attribute='cobraType', null=True, readonly=True) # used, but is the optimization correct? queryset > 20?

	class Meta:
		from register_company.models import Employee
		from company_enrollment.models import CompanyHealthEnrollment, PRIMARY_BENEFITS

		currentHealthEnrollmentsQuery = CompanyHealthEnrollment.objects.filter(
			lineOfCoverage__in=PRIMARY_BENEFITS,
			enrollmentStatus__in=('complete', 'document', 'switched'),
			isActive=True,
			isEnrollmentComplete=True
		).select_related('healthCarrier')

		employeesQuery = Employee.objects.filter(isEligibleForHealth=True)
		cobraEmployeesQuery = Employee.objects.filter(status='Ter', employeeCobra__status__in=('submitted', 'enrolled')).select_related('employeeCobra')
		enrollmentsQuery = EmployeeHealthEnrollment.objects.filter(
			isActive=True,
			coverage_type__in=PRIMARY_BENEFITS,
		).only('coverage_type', 'effectiveDate')

		queryset = CompanySettings.objects.all()
		select_related = ('company__companyStatus', 'company__policy')
		prefetch_related = (
			Prefetch('company__healthEnrollments', queryset=currentHealthEnrollmentsQuery, to_attr='currentHealthEnrollments'),
			Prefetch('company__employees', queryset=employeesQuery, to_attr='ftEmployees'),
			Prefetch('company__employees', queryset=cobraEmployeesQuery, to_attr='cobraEmployees'),
			Prefetch('company__ftEmployees__enrollments', queryset=enrollmentsQuery, to_attr='activeEnrollments'),
		)

		resource_name = 'company_settings'
		authorization = Authorization()
		always_return_data = True
		include_resource_uri = False
		limit = 0
		default_format = "application/json"

	def determine_format(self, request):
		"""
		Used to determine the desired format from the request.format
		attribute.
		"""
		if (hasattr(request, 'format') and
				request.format in self._meta.serializer.formats):
			return self._meta.serializer.get_mime_for_format(request.format)
		return "application/json"


	def dehydrate(self, bundle):
		if not bundle.obj:
			return bundle

		bundle.data['company_id'] = bundle.obj.company_id
		employees = bundle.obj.getNumOldEmployees(getList=True)

		# Batched loading of these two properties because they overlap.
		# Probably more improvements to be made here.
		isOpenEnrollmentInProgress = bundle.obj.isOpenEnrollmentInProgress
		bundle.data['isOpenEnrollmentInProgress'] = isOpenEnrollmentInProgress
		bundle.data['isAnyEnrollmentInProgress'] = bundle.obj._isAnyEnrollmentInProgress(isOpenEnrollmentInProgress)

		# these use the same underlying data but will make the SQL queries twice.. :(
		oeStart = bundle.obj.openEnrollmentStart
		bundle.data['openEnrollmentStartDate'] = bundle.obj._openEnrollmentStartDateDisplay(start=oeStart) if oeStart else None
		bundle.data['openEnrollmentEndDate'] = bundle.obj._openEnrollmentEndDateDisplay(start=oeStart) if oeStart else None
		bundle.data['openEnrollmentMonth'] = bundle.obj._openEnrollmentMonth(start=oeStart) if oeStart else None

		if bundle.obj.approvedDate:
			try:
				approvedDate = bundle.obj.approvedDate
				bundle.data['approvedDate'] = approvedDate.strftime('%m/%d/%Y')
			except:
				traceback.print_exc()
				bundle.data['approvedDate'] = bundle.obj.approvedDate

		# batch the loading for these 3?
		medicalEnrollmentEndDate = bundle.obj.medicalEnrollmentEndDate
		if medicalEnrollmentEndDate:
			bundle.data['medicalEnrollmentEndDate'] = formatUSDate(medicalEnrollmentEndDate)

		dentalEnrollmentEndDate = bundle.obj.dentalEnrollmentEndDate
		if dentalEnrollmentEndDate:
			bundle.data['dentalEnrollmentEndDate'] = formatUSDate(dentalEnrollmentEndDate)

		visionEnrollmentEndDate = bundle.obj.visionEnrollmentEndDate
		if visionEnrollmentEndDate:
			bundle.data['visionEnrollmentEndDate'] = formatUSDate(visionEnrollmentEndDate)

		if bundle.obj.cobraTakeoverDate:
			bundle.data['cobraTakeoverDate'] = formatUSDate(bundle.obj.cobraTakeoverDate)

		company = bundle.obj.company
		bundle.data['isSwitchCompany'] = (company.enrollmentStatus == 'switched')

		bundle.data['isDraftUploadComplete'] = bundle.obj.isDraftUploadComplete

		today = datetime.date.today()
		employeeCount = 0
		for e in employees:
			for ehe in e.activeEnrollments:
				if (ehe.endDate and ehe.endDate >= today and
					(ehe.status not in ('complete', 'reviewed', 'decline') or (ehe.status == 'decline' and not ehe.hasSignedWaiver))):

					employeeCount += 1
					break

		bundle.data['numOldEmployees'] = len(employees)
		bundle.data['numOldEmployeesInProgress'] = employeeCount

		bundle.data['isLifeEnrollmentInProgress'] = bundle.obj.isLifeEnrollmentInProgress(employees=employees)
		bundle.data['isAddEnrollmentInProgress'] = bundle.obj.isAddEnrollmentInProgress(employees=employees)
		bundle.data['isLtdEnrollmentInProgress'] = bundle.obj.isLtdEnrollmentInProgress(employees=employees)
		bundle.data['isStdEnrollmentInProgress'] = bundle.obj.isStdEnrollmentInProgress(employees=employees)

		# TODO(zmohan): Remove medical/dental/vision completed employees count after ZApps migration
		enrollmentsMap = {
			'medical': {},
			'dental': {},
			'vision': {}
		}

		for e in employees:
			for ehe in e.activeEnrollments:
				enrollmentMap = enrollmentsMap.get(ehe.coverage_type)
				enrollmentMap[ehe.employee_id] = ehe

		medicalCompletedEmployeeCount = 0
		dentalCompletedEmployeeCount = 0
		visionCompletedEmployeeCount = 0

		if company.medicalEnrollmentStatus in ('complete', 'document'):
			for employee in employees:
				medicalEnrollmentsMap = enrollmentsMap.get('medical')
				ehe = medicalEnrollmentsMap.get(employee.id, None)
				if ehe is None:
					if employee.medicalStatus in ('complete', 'decline'):
						medicalCompletedEmployeeCount += 1
				else:
					if ehe.status == 'complete' or (ehe.status == 'decline' and ehe.hasSignedWaiver):
						medicalCompletedEmployeeCount += 1

		if company.dentalEnrollmentStatus in ('complete', 'document'):
			for employee in employees:
				dentalEnrollmentsMap = enrollmentsMap.get('dental')
				ehe = dentalEnrollmentsMap.get(employee.id, None)
				if ehe is None:
					if employee.dentalStatus in ('complete', 'decline'):
						dentalCompletedEmployeeCount += 1
				else:
					if ehe.status == 'complete' or (ehe.status == 'decline' and ehe.hasSignedWaiver):
						dentalCompletedEmployeeCount += 1

		if company.visionEnrollmentStatus in ('complete', 'document'):
			for employee in employees:
				visionEnrollmentsMap = enrollmentsMap.get('vision')
				ehe = visionEnrollmentsMap.get(employee.id, None)
				if ehe is None:
					if employee.visionStatus in ('complete', 'decline'):
						visionCompletedEmployeeCount += 1
				else:
					if ehe.status == 'complete' or (ehe.status == 'decline' and ehe.hasSignedWaiver):
						visionCompletedEmployeeCount += 1

		bundle.data['totalEmployeeCount'] = len(employees)
		bundle.data['medicalCompletedEmployeeCount'] = medicalCompletedEmployeeCount
		bundle.data['dentalCompletedEmployeeCount'] = dentalCompletedEmployeeCount
		bundle.data['visionCompletedEmployeeCount'] = visionCompletedEmployeeCount

		bundle.data['areEmployeesFedCobraEligible'] = True if bundle.obj.cobraType == 'Federal' else False

		wasCobraBorEmailSent = bundle.obj.wasCobraBorEmailSent
		if wasCobraBorEmailSent:
			bundle.data['wasCobraBorEmailSent'] = formatUSDate(wasCobraBorEmailSent)

		return bundle

	def apply_authorization_limits(self, request, object_list):
		# TODO: accept console_view permissions for drafts.
		# Unified permissions scheme coming.
		if request.user.has_perm('work_queue.workitem_edit') or request.user.has_perm('register_company.console_view'):
			return object_list

		try:
			if Company.isAdminForRequest(request):
				return object_list.filter(company_id=request.company_id)
		except Company.DoesNotExist:
			pass

		try:
			employee = Employee.objects.get(id=request.employee_id)
			return object_list.filter(company_id=employee.company_id)
		except:
			raise NotFound('Not found')

class StaffDirectoryResource(ZenefitsPeopleApiMixin, ModelResource):
	class Meta:
		queryset = CompanySettings.objects.all()
		fields = ['showStaffDirectory']
		filtering = {
			'company_id': 'exact',
		}
		allowed_methods = ['get']
		resource_name = 'staff_directory'

	def apply_authorization_limits(self, request, object_list):
		return filterByCompanyOrEmployee(request, object_list)


class ManualEmployeeResource(ModelResource):

	class Meta:
		queryset = Employee.objects.all()
		resource_name = 'manual_employee'
		authorization = Authorization()
		always_return_data = True
		include_resource_uri = False
		fields = ['id', 'first_name', 'last_name', 'zip', 'ageRange', 'status', 'employmentType', 'creationMethod']
		limit = 0
		default_format = "application/json"

	def determine_format(self, request):
		"""
		Used to determine the desired format from the request.format
		attribute.
		"""
		if (hasattr(request, 'format') and
				request.format in self._meta.serializer.formats):
			return self._meta.serializer.get_mime_for_format(request.format)
		return "application/json"

	def obj_delete(self, request=None, **kwargs):
		identifier = kwargs['pk']
		employee = Employee.objects.get(pk=identifier)
		employee.delete()

	def hydrate_zip(self, bundle):
		zipCode = bundle.data['zip']
		if zipCode:
			try:
				zipCode = int(zipCode)
				mapping = zipcountyservice.getCountyForZip(zipCode)
				bundle.obj.state = mapping.state
				bundle.data['state'] = mapping.state
			except:
				bundle.obj.state = ''
				bundle.data['state'] = ''

		return bundle

	def obj_update(self, bundle, request=None, **kwargs):
		identifier = kwargs['pk']
		company = Company.fromRequest(request)
		bundle.obj.company = company
		bundle.obj.id = identifier
		return super(ManualEmployeeResource, self).obj_update(bundle, request, company=company)

	def obj_create(self, bundle, request=None, **kwargs):
		company = Company.fromRequest(request)
		bundle.obj.company = company
		bundle = super(ManualEmployeeResource, self).obj_create(bundle, request, company=company)
		try:
			bundle.obj.setUser()
		except:
			pass

		return bundle

	def apply_authorization_limits(self, request, object_list):
		if request.user.has_perm('work_queue.workitem_edit'):
			return object_list

		try:
			if Company.isAdminForRequest(request):
				return object_list.filter(company_id=request.company_id)
		except:
			return []

class BreakdownResource(SalesDemoCacheGetMixin, ExtendedResource):
	id = fields.IntegerField(attribute='id', readonly=True, null=True)
	carrier = fields.CharField(attribute='carrier', readonly=True, null=True)
	numberEnrolled = fields.IntegerField(attribute='numberEnrolled', readonly=True, null=True)
	contributionType = fields.CharField(attribute='contributionType', readonly=True, null=True)
	employeeContribution = fields.CharField(attribute='employeeContribution', readonly=True, null=True)
	dependentContribution = fields.CharField(attribute='dependentContribution', readonly=True, null=True)
	employeeTotalCost = fields.DecimalField(attribute='employeeTotalCost', readonly=True, null=True)
	dependentsTotalCost = fields.DecimalField(attribute='dependentsTotalCost', readonly=True, null=True)
	employeeCompanyCost = fields.DecimalField(attribute='employeeCompanyCost', readonly=True, null=True)
	dependentsCompanyCost = fields.DecimalField(attribute='dependentsCompanyCost', readonly=True, null=True)
	employeeCost = fields.DecimalField(attribute='employeeCost', readonly=True, null=True)
	dependentsCost = fields.DecimalField(attribute='dependentsCost', readonly=True, null=True)
	totalCompanyFee = fields.DecimalField(attribute='totalCompanyFee', readonly=True, null=True)
	totalEmployeeFee = fields.DecimalField(attribute='totalEmployeeFee', readonly=True, null=True)
	totalPremium = fields.DecimalField(attribute='totalPremium', readonly=True, null=True)
